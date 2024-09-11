use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    storage,
};
use dashmap::DashMap;
use message::MessageBatch;
use sqlx::AnyPool;
use tokio::sync::{broadcast, mpsc};
use types::{
    devices::{
        CreateUpdateDeviceReq, DeviceConf, DeviceType, QueryParams, QueryRuleInfo,
        SearchDevicesItemCommon, SearchDevicesItemConf, SearchDevicesItemFromMemory,
        SearchDevicesItemResp, SearchDevicesResp, SearchRuleInfo, Summary,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksInfoResp,
    SearchSourcesOrSinksResp, Value,
};

use uuid::Uuid;

pub mod coap;
pub mod modbus;
pub mod opcua;

static DEVICE_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static DEVICE_ON_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static DEVICE_RUNNING_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

fn get_device_count() -> usize {
    DEVICE_COUNT.load(Ordering::SeqCst)
}

fn add_device_count() {
    DEVICE_COUNT.fetch_add(1, Ordering::SeqCst);
}

fn sub_device_count() {
    DEVICE_COUNT.fetch_sub(1, Ordering::SeqCst);
}

pub(crate) fn get_device_on_count() -> usize {
    DEVICE_ON_COUNT.load(Ordering::SeqCst)
}

pub(crate) fn add_device_on_count() {
    DEVICE_ON_COUNT.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn sub_device_on_count() {
    DEVICE_ON_COUNT.fetch_sub(1, Ordering::SeqCst);
}

pub(crate) fn get_device_running_count() -> usize {
    DEVICE_RUNNING_COUNT.load(Ordering::SeqCst)
}

pub(crate) fn add_device_running_count() {
    DEVICE_RUNNING_COUNT.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn sub_device_running_count() {
    DEVICE_RUNNING_COUNT.fetch_sub(1, Ordering::SeqCst);
}

#[async_trait]
pub trait Device: Send + Sync {
    fn get_id(&self) -> &Uuid;
    fn check_duplicate(&self, req: &CreateUpdateDeviceReq) -> HaliaResult<()>;
    async fn read(&self) -> SearchDevicesItemFromMemory;
    async fn update(&mut self, old_conf: String, new_conf: &serde_json::Value) -> HaliaResult<()>;
    async fn stop(&mut self) -> HaliaResult<()>;

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: &CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn search_sources(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp;
    async fn read_source(&self, source_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp>;
    async fn update_source(
        &mut self,
        source_id: Uuid,
        old_conf: String,
        req: &CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn write_source_value(&mut self, source_id: Uuid, req: Value) -> HaliaResult<()>;
    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()>;

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn search_sinks(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp;
    async fn read_sink(&self, sink_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp>;
    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()>;

    fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>>;
    fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;

    fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>>;
    fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
}

pub async fn load_from_storage(
    storage: &Arc<AnyPool>,
) -> HaliaResult<Arc<DashMap<Uuid, Box<dyn Device>>>> {
    let db_devices = storage::device::read_on_devices(storage).await?;
    let devices: Arc<DashMap<Uuid, Box<dyn Device>>> = Arc::new(DashMap::new());

    for db_device in db_devices {
        let device_id = Uuid::from_str(&db_device.id).unwrap();

        start_device(storage, &devices, device_id).await?;

        let db_sources = storage::source::read_sources(storage, &device_id).await?;
        let db_sinks = storage::sink::read_sinks(storage, &device_id).await?;

        for db_source in db_sources {
            let req = CreateUpdateSourceOrSinkReq {
                base: BaseConf {
                    name: db_source.name,
                    desc: db_source.desc,
                },
                ext: serde_json::to_value(db_source.conf).unwrap(),
            };
            create_source(
                storage,
                &devices,
                device_id,
                Uuid::from_str(&db_source.id).unwrap(),
                req,
                false,
            )
            .await?;
        }

        for db_sink in db_sinks {
            let req = CreateUpdateSourceOrSinkReq {
                base: BaseConf {
                    name: db_sink.name,
                    desc: db_sink.desc,
                },
                ext: serde_json::to_value(db_sink.conf).unwrap(),
            };
            create_sink(
                storage,
                &devices,
                device_id,
                Uuid::from_str(&db_sink.id).unwrap(),
                req,
                false,
            )
            .await?;
        }
    }

    Ok(devices)
}

pub fn get_summary() -> Summary {
    let total = get_device_count();
    let on = get_device_on_count();
    let running = get_device_running_count();

    Summary { total, on, running }
}

pub async fn get_rule_info(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    query: QueryRuleInfo,
) -> HaliaResult<SearchRuleInfo> {
    todo!()

    // match devices
    //     .read()
    //     .await
    //     .iter()
    //     .find(|device| *device.get_id() == query.device_id)
    // {
    //     Some(device) => {
    //         let device_info = device.read().await;
    //         match (query.source_id, query.sink_id) {
    //             (Some(source_id), None) => {
    //                 let source_info = device.read_source(&source_id).await?;
    //                 Ok(SearchRuleInfo {
    //                     device: device_info,
    //                     source: Some(source_info),
    //                     sink: None,
    //                 })
    //             }
    //             (None, Some(sink_id)) => {
    //                 let sink_info = device.read_sink(&sink_id).await?;
    //                 Ok(SearchRuleInfo {
    //                     device: device_info,
    //                     source: None,
    //                     sink: Some(sink_info),
    //                 })
    //             }
    //             _ => {
    //                 return Err(HaliaError::Common(
    //                     "查询source_id或sink_id参数错误！".to_string(),
    //                 ))
    //             }
    //         }
    //     }
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn create_device(
    storage: &Arc<AnyPool>,
    device_id: Uuid,
    req: CreateUpdateDeviceReq,
) -> HaliaResult<()> {
    // let db_req = req.clone();
    // for device in devices.read().await.iter() {
    //     device.check_duplicate(&req)?;
    // }

    match &req.device_type {
        DeviceType::Modbus => modbus::validate_conf(&req.conf.ext)?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
        // DeviceType::Opcua => opcua::new(device_id, req.conf).await?,
        // DeviceType::Coap => coap::new(device_id, req.conf).await?,
    }
    add_device_count();
    storage::device::create_device(&storage, &device_id, req).await?;
    Ok(())
}

pub async fn search_devices(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<SearchDevicesResp> {
    let (count, db_devices) = storage::device::search_devices(storage, pagination).await?;
    let mut resp_devices = vec![];
    for db_device in db_devices {
        let device_id = Uuid::from_str(&db_device.id).unwrap();

        let memory_info = match db_device.status {
            0 => None,
            1 => Some(devices.get(&device_id).unwrap().read().await),
            _ => unreachable!(),
        };

        let source_cnt = storage::source::count_sources_by_parent_id(storage, &device_id).await?;
        let sink_cnt = storage::sink::count_sinks_by_parent_id(storage, &device_id).await?;

        // 从内存中获取设备信息
        resp_devices.push(SearchDevicesItemResp {
            common: SearchDevicesItemCommon {
                id: device_id,
                device_type: DeviceType::try_from(db_device.device_type).unwrap(),
                on: db_device.status == 1,
                source_cnt,
                sink_cnt,
                memory_info,
            },
            conf: SearchDevicesItemConf {
                base: BaseConf {
                    name: db_device.name,
                    desc: db_device.desc,
                },
                ext: serde_json::from_str(&db_device.conf).unwrap(),
            },
        });
    }
    // for device in devices.read().await.iter().rev() {
    //     let device = device.read().await;
    //     if let Some(device_type) = &query_params.device_type {
    //         if *device_type != device.common.device_type {
    //             continue;
    //         }
    //     }

    //     if let Some(name) = &query_params.name {
    //         if !device.conf.base.name.contains(name) {
    //             continue;
    //         }
    //     }

    //     if let Some(on) = &query_params.on {
    //         if device.common.on != *on {
    //             continue;
    //         }
    //     }

    //     if let Some(err) = &query_params.err {
    //         if device.common.err.is_some() != *err {
    //             continue;
    //         }
    //     }

    //     if pagination.check(total) {
    //         data.push(device);
    //     }

    //     total += 1;
    // }

    Ok(SearchDevicesResp {
        total: count,
        data: resp_devices,
    })
}

pub async fn update_device(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    req: CreateUpdateDeviceReq,
) -> HaliaResult<()> {
    if let Some(mut device) = devices.get_mut(&device_id) {
        let db_device = storage::device::read_device(storage, &device_id).await?;
        device.update(db_device.conf, &req.conf.ext).await?;
    }

    storage::device::update_device_conf(storage, &device_id, req).await?;

    Ok(())
}

pub async fn start_device(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
) -> HaliaResult<()> {
    // 设备已启动
    if devices.contains_key(&device_id) {
        return Ok(());
    }

    let db_device = storage::device::read_device(storage, &device_id).await?;
    let device_type = DeviceType::try_from(db_device.device_type)?;

    // TODO 取消unwrap
    let device_id = Uuid::from_str(&db_device.id).unwrap();
    let device_conf: DeviceConf = DeviceConf {
        base: BaseConf {
            name: db_device.name,
            desc: db_device.desc,
        },
        ext: serde_json::from_str(&db_device.conf).unwrap(),
    };

    let device = match device_type {
        DeviceType::Modbus => modbus::new(device_id, device_conf, storage.clone()),
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    };
    devices.insert(device_id, device);

    add_device_on_count();
    storage::device::update_device_status(storage, &device_id, true).await?;
    Ok(())
}

pub async fn stop_device(
    persistence: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
) -> HaliaResult<()> {
    // 设备已停止
    if !devices.contains_key(&device_id) {
        return Ok(());
    }

    devices
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .stop()
        .await?;

    devices.remove(&device_id);

    storage::device::update_device_status(persistence, &device_id, false).await?;

    Ok(())
}

pub async fn delete_device(
    persistence: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
) -> HaliaResult<()> {
    if devices.contains_key(&device_id) {
        return Err(HaliaError::Common("运行中，不能删除".to_string()));
    }

    // todo 判断引用

    // TODO 停止时
    // devices
    //     .get_mut(&device_id)
    //     .ok_or(HaliaError::NotFound)?
    //     .delete()
    //     .await?;

    sub_device_count();
    devices.remove(&device_id);
    storage::device::delete_device(persistence, &device_id).await?;

    Ok(())
}

pub async fn create_source(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    source_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
    persist: bool,
) -> HaliaResult<()> {
    if let Some(mut device) = devices.get_mut(&device_id) {
        device.create_source(source_id, &req).await?;
    }

    if persist {
        storage::source::create_source(storage, &device_id, &source_id, req).await?;
    }

    Ok(())
}

pub async fn search_sources(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    Ok(devices
        .get(&device_id)
        .ok_or(HaliaError::NotFound)?
        .search_sources(pagination, query)
        .await)

    // match devices
    //     .read()
    //     .await
    //     .iter()
    //     .find(|device| *device.get_id() == device_id)
    // {
    //     Some(device) => Ok(device.search_sources(pagination, query).await),
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn update_source(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    source_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    let old_conf = storage::source::read_source_conf(storage, &source_id).await?;
    devices
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .update_source(source_id, old_conf, &req)
        .await?;

    storage::source::update_source(storage, &source_id, req).await?;

    Ok(())
}

pub async fn write_source_value(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    source_id: Uuid,
    req: Value,
) -> HaliaResult<()> {
    devices
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .write_source_value(source_id, req)
        .await
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == device_id)
    // {
    //     Some(device) => device.write_source_value(source_id, req).await,
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn delete_source(
    persistence: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    source_id: Uuid,
) -> HaliaResult<()> {
    devices
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .delete_source(source_id)
        .await?;
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == device_id)
    // {
    //     Some(device) => device.delete_source(source_id).await?,
    //     None => return Err(HaliaError::NotFound),
    // }

    storage::source::delete_source(persistence, &source_id).await?;

    Ok(())
}

pub async fn add_source_ref(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .get_mut(device_id)
        .expect("device not found")
        .add_source_ref(source_id, rule_id)
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == *device_id)
    // {
    //     Some(device) => device.add_source_ref(source_id, rule_id),
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn get_source_rx(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
    devices
        .get_mut(device_id)
        .ok_or(HaliaError::NotFound)?
        .get_source_rx(source_id, rule_id)
        .await

    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == *device_id)
    // {
    //     Some(device) => device.get_source_rx(source_id, rule_id).await,
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn del_source_rx(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    devices
        .get_mut(device_id)
        .ok_or(HaliaError::NotFound)?
        .del_source_rx(source_id, rule_id)
}

pub async fn del_source_ref(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .get_mut(device_id)
        .expect("device not found")
        .del_source_ref(source_id, rule_id)
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == *device_id)
    // {
    //     Some(device) => device.del_source_ref(source_id, rule_id),
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn create_sink(
    persistence: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    sink_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
    persist: bool,
) -> HaliaResult<()> {
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == device_id)
    // {
    //     Some(device) => device.create_sink(sink_id, req).await?,
    //     None => return Err(HaliaError::NotFound),
    // }

    if persist {
        storage::sink::create_sink(persistence, &device_id, &sink_id, req).await?;
    }

    Ok(())
}

pub async fn search_sinks(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    Ok(devices
        .get(&device_id)
        .ok_or(HaliaError::NotFound)?
        .search_sinks(pagination, query)
        .await)
    // match devices
    //     .read()
    //     .await
    //     .iter()
    //     .find(|device| *device.get_id() == device_id)
    // {
    //     Some(device) => Ok(device.search_sinks(pagination, query).await),
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn update_sink(
    pool: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    sink_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    devices
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .update_sink(sink_id, req)
        .await?;
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == device_id)
    // {
    //     Some(device) => device.update_sink(sink_id, req).await?,
    //     None => return Err(HaliaError::NotFound),
    // }

    storage::sink::update_sink(pool, &sink_id, body).await?;

    Ok(())
}

pub async fn delete_sink(
    pool: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    sink_id: Uuid,
) -> HaliaResult<()> {
    devices
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .delete_sink(sink_id)
        .await?;

    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == device_id)
    // {
    //     Some(device) => device.delete_sink(sink_id).await?,
    //     None => return Err(HaliaError::NotFound),
    // }

    storage::sink::delete_sink(pool, &sink_id).await?;

    Ok(())
}

pub async fn add_sink_ref(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .get_mut(device_id)
        .expect("device not found")
        .add_sink_ref(sink_id, rule_id)
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == *device_id)
    // {
    //     Some(device) => device.add_sink_ref(sink_id, rule_id),
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn get_sink_tx(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    devices
        .get_mut(device_id)
        .ok_or(HaliaError::NotFound)?
        .get_sink_tx(sink_id, rule_id)
        .await
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == *device_id)
    // {
    //     Some(device) => device.get_sink_tx(sink_id, rule_id).await,
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn del_sink_tx(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    devices
        .get_mut(device_id)
        .ok_or(HaliaError::NotFound)?
        .del_sink_tx(sink_id, rule_id)
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == *device_id)
    // {
    //     Some(device) => device.del_sink_tx(sink_id, rule_id),
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn del_sink_ref(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .get_mut(device_id)
        .expect("device not found")
        .del_sink_ref(sink_id, rule_id)
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
    // match devices
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|device| *device.get_id() == *device_id)
    // {
    //     Some(device) => device.del_sink_ref(sink_id, rule_id),
    //     None => Err(HaliaError::NotFound),
    // }
}

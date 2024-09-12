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
use tracing::{debug, warn};
use types::{
    devices::{
        CreateUpdateDeviceReq, DeviceConf, DeviceType, QueryParams, QueryRuleInfo,
        SearchDevicesItemCommon, SearchDevicesItemConf, SearchDevicesItemFromMemory,
        SearchDevicesItemResp, SearchDevicesResp, SearchRuleInfo, Summary,
    },
    events::EventType,
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams, RuleRef,
    SearchSourcesOrSinksInfoResp, SearchSourcesOrSinksItemResp, SearchSourcesOrSinksResp, Value,
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
    async fn read(&self) -> SearchDevicesItemFromMemory;
    async fn update(&mut self, old_conf: String, new_conf: &serde_json::Value) -> HaliaResult<()>;
    async fn stop(&mut self) -> HaliaResult<()>;

    async fn create_source(&mut self, source_id: Uuid, conf: serde_json::Value) -> HaliaResult<()>;
    async fn update_source(
        &mut self,
        source_id: Uuid,
        old_conf: String,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn write_source_value(&mut self, source_id: Uuid, req: Value) -> HaliaResult<()>;
    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()>;

    async fn create_sink(&mut self, sink_id: Uuid, conf: serde_json::Value) -> HaliaResult<()>;
    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        old_conf: String,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()>;

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>>;

    async fn get_sink_tx(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>>;
}

pub async fn load_from_storage(
    storage: &Arc<AnyPool>,
) -> HaliaResult<Arc<DashMap<Uuid, Box<dyn Device>>>> {
    let count = storage::device::count_all(storage).await?;
    DEVICE_COUNT.store(count, Ordering::SeqCst);

    let db_devices = storage::device::read_on(storage).await?;
    DEVICE_ON_COUNT.store(db_devices.len(), Ordering::SeqCst);

    let devices: Arc<DashMap<Uuid, Box<dyn Device>>> = Arc::new(DashMap::new());
    for db_device in db_devices {
        let device_id = Uuid::from_str(&db_device.id).unwrap();
        start_device(storage, &devices, device_id).await?;
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
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    query: QueryRuleInfo,
) -> HaliaResult<SearchRuleInfo> {
    let db_device = storage::device::read_device(storage, &query.device_id).await?;

    let device_id = Uuid::from_str(&db_device.id).unwrap();
    let memory_info = match db_device.status {
        0 => None,
        1 => Some(devices.get(&device_id).unwrap().read().await),
        _ => unreachable!(),
    };
    let device_resp = SearchDevicesItemResp {
        common: SearchDevicesItemCommon {
            id: db_device.id,
            device_type: DeviceType::try_from(db_device.device_type).unwrap(),
            on: db_device.status == 1,
            source_cnt: storage::source_or_sink::count_by_parent_id(
                storage,
                &query.device_id,
                storage::source_or_sink::Type::Source,
            )
            .await?,
            sink_cnt: storage::source_or_sink::count_by_parent_id(
                storage,
                &query.device_id,
                storage::source_or_sink::Type::Sink,
            )
            .await?,
            memory_info,
        },
        conf: SearchDevicesItemConf {
            base: BaseConf {
                name: db_device.name,
                desc: db_device.desc,
            },
            ext: serde_json::from_str(&db_device.conf).unwrap(),
        },
    };
    let id = match (query.source_id, query.sink_id) {
        (Some(source_id), None) => source_id,
        (None, Some(sink_id)) => sink_id,
        _ => {
            return Err(HaliaError::Common(
                "查询source_id或sink_id参数错误！".to_string(),
            ))
        }
    };

    let db_source_or_sink = storage::source_or_sink::read(storage, &id).await?;
    Ok(SearchRuleInfo {
        device: device_resp,
        source: Some(SearchSourcesOrSinksInfoResp {
            id: Uuid::from_str(&db_source_or_sink.id).unwrap(),
            conf: CreateUpdateSourceOrSinkReq {
                base: BaseConf {
                    name: db_source_or_sink.name,
                    desc: db_source_or_sink.desc,
                },
                ext: serde_json::from_str(&db_source_or_sink.conf).unwrap(),
            },
        }),
        sink: None,
    })
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
    // TODO 查询异常设备
    let (count, db_devices) =
        storage::device::search_devices(storage, pagination, query_params).await?;
    let mut resp_devices = vec![];
    for db_device in db_devices {
        let device_id = Uuid::from_str(&db_device.id).unwrap();

        let memory_info = match db_device.status {
            0 => None,
            1 => Some(devices.get(&device_id).unwrap().read().await),
            _ => unreachable!(),
        };
        resp_devices.push(SearchDevicesItemResp {
            common: SearchDevicesItemCommon {
                id: db_device.id,
                device_type: DeviceType::try_from(db_device.device_type).unwrap(),
                on: db_device.status == 1,
                source_cnt: storage::source_or_sink::count_by_parent_id(
                    storage,
                    &device_id,
                    storage::source_or_sink::Type::Source,
                )
                .await?,
                sink_cnt: storage::source_or_sink::count_by_parent_id(
                    storage,
                    &device_id,
                    storage::source_or_sink::Type::Sink,
                )
                .await?,
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

    storage::device::create_event(storage, &device_id, EventType::Start.into(), None).await?;

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

    debug!("{:?}", device_conf);

    let device = match device_type {
        DeviceType::Modbus => modbus::new(device_id, device_conf, storage.clone()),
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    };
    devices.insert(device_id, device);

    let mut device = devices.get_mut(&device_id).unwrap();

    let db_sources = storage::source_or_sink::read_all_by_parent_id(
        storage,
        &device_id,
        storage::source_or_sink::Type::Source,
    )
    .await?;
    for db_source in db_sources {
        let conf: serde_json::Value = serde_json::from_str(&db_source.conf).unwrap();
        device
            .create_source(Uuid::from_str(&db_source.id).unwrap(), conf)
            .await?;
    }

    let db_sinks = storage::source_or_sink::read_all_by_parent_id(
        storage,
        &device_id,
        storage::source_or_sink::Type::Sink,
    )
    .await?;
    for db_sink in db_sinks {
        let conf: serde_json::Value = serde_json::from_str(&db_sink.conf).unwrap();
        device
            .create_sink(Uuid::from_str(&db_sink.id).unwrap(), conf)
            .await?;
    }

    add_device_on_count();
    storage::device::update_device_status(storage, &device_id, true).await?;
    Ok(())
}

pub async fn stop_device(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
) -> HaliaResult<()> {
    // 设备已停止
    if !devices.contains_key(&device_id) {
        return Ok(());
    }

    let active_rule_ref_cnt =
        storage::rule_ref::count_active_cnt_by_parent_id(storage, &device_id).await?;
    if active_rule_ref_cnt > 0 {
        return Err(HaliaError::StopActiveRefing);
    }

    if let Err(e) = storage::device::create_event(
        storage,
        &device_id,
        types::events::EventType::Stop.into(),
        None,
    )
    .await
    {
        warn!("create event failed: {}", e);
    }

    storage::device::update_device_status(storage, &device_id, false).await?;
    devices.get_mut(&device_id).unwrap().stop().await?;

    devices.remove(&device_id);
    sub_device_on_count();

    Ok(())
}

pub async fn delete_device(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
) -> HaliaResult<()> {
    if devices.contains_key(&device_id) {
        return Err(HaliaError::Common("运行中，不能删除".to_string()));
    }

    let cnt = storage::rule_ref::count_cnt_by_parent_id(storage, &device_id).await?;
    if cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    sub_device_count();
    storage::device::delete_device(storage, &device_id).await?;
    storage::source_or_sink::delete_by_parent_id(storage, &device_id).await?;

    Ok(())
}

pub async fn create_source(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    let source_id = Uuid::new_v4();
    if let Some(mut device) = devices.get_mut(&device_id) {
        let conf = req.ext.clone();
        device.create_source(source_id, conf).await?;
    }

    storage::source_or_sink::create(
        storage,
        &device_id,
        &source_id,
        storage::source_or_sink::Type::Source,
        req,
    )
    .await?;

    Ok(())
}

pub async fn search_sources(
    storage: &Arc<AnyPool>,
    device_id: Uuid,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sources) = storage::source_or_sink::search(
        storage,
        &device_id,
        storage::source_or_sink::Type::Source,
        pagination,
        query,
    )
    .await?;
    let mut data = Vec::with_capacity(db_sources.len());
    for db_source in db_sources {
        let id = Uuid::from_str(&db_source.id).unwrap();
        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id: Uuid::from_str(&db_source.id).unwrap(),
                conf: CreateUpdateSourceOrSinkReq {
                    base: BaseConf {
                        name: db_source.name,
                        desc: db_source.desc,
                    },
                    ext: serde_json::from_str(&db_source.conf).unwrap(),
                },
            },
            rule_ref: RuleRef {
                rule_ref_cnt: storage::rule_ref::count_cnt_by_resource_id(storage, &id).await?,
                rule_active_ref_cnt: storage::rule_ref::count_active_cnt_by_resource_id(
                    storage, &id,
                )
                .await?,
            },
        });
    }

    Ok(SearchSourcesOrSinksResp { total: count, data })
}

pub async fn update_source(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    source_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    let old_conf = storage::source_or_sink::read_conf_by_id(storage, &source_id).await?;
    let new_conf = req.ext.clone();
    devices
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .update_source(source_id, old_conf, new_conf)
        .await?;

    storage::source_or_sink::update(storage, &source_id, req).await?;

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
}

pub async fn delete_source(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    source_id: Uuid,
) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule_ref::count_cnt_by_resource_id(storage, &source_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    if let Some(mut device) = devices.get_mut(&device_id) {
        device.delete_source(source_id).await?;
    }

    storage::source_or_sink::delete(storage, &source_id).await?;

    Ok(())
}

pub async fn get_source_rx(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    source_id: &Uuid,
) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
    devices
        .get_mut(device_id)
        .ok_or(HaliaError::Stopped)?
        .get_source_rx(source_id)
        .await
}

pub async fn create_sink(
    persistence: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    let sink_id = Uuid::new_v4();
    if let Some(mut device) = devices.get_mut(&device_id) {
        let conf: serde_json::Value = req.ext.clone();
        device.create_sink(sink_id, conf).await?;
    }

    storage::source_or_sink::create(
        persistence,
        &device_id,
        &sink_id,
        storage::source_or_sink::Type::Sink,
        req,
    )
    .await?;

    Ok(())
}

pub async fn search_sinks(
    storage: &Arc<AnyPool>,
    device_id: Uuid,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sinks) = storage::source_or_sink::search(
        storage,
        &device_id,
        storage::source_or_sink::Type::Sink,
        pagination,
        query,
    )
    .await?;
    let mut data = Vec::with_capacity(db_sinks.len());
    for db_sink in db_sinks {
        let id = Uuid::from_str(&db_sink.id).unwrap();

        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id,
                conf: CreateUpdateSourceOrSinkReq {
                    base: BaseConf {
                        name: db_sink.name,
                        desc: db_sink.desc,
                    },
                    ext: serde_json::from_str(&db_sink.conf).unwrap(),
                },
            },
            rule_ref: RuleRef {
                rule_ref_cnt: storage::rule_ref::count_cnt_by_resource_id(storage, &id).await?,
                rule_active_ref_cnt: storage::rule_ref::count_active_cnt_by_resource_id(
                    storage, &id,
                )
                .await?,
            },
        });
    }

    Ok(SearchSourcesOrSinksResp { total: count, data })
}

pub async fn update_sink(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    sink_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    let old_conf = storage::source_or_sink::read_conf_by_id(storage, &sink_id).await?;
    let new_conf = req.ext.clone();
    devices
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .update_sink(sink_id, old_conf, new_conf)
        .await?;

    storage::source_or_sink::update(storage, &sink_id, req).await?;

    Ok(())
}

pub async fn delete_sink(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: Uuid,
    sink_id: Uuid,
) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule_ref::count_cnt_by_resource_id(storage, &sink_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    if let Some(mut device) = devices.get_mut(&device_id) {
        device.delete_sink(sink_id).await?;
    }

    storage::source_or_sink::delete(storage, &sink_id).await?;

    Ok(())
}

pub async fn get_sink_tx(
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    device_id: &Uuid,
    sink_id: &Uuid,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    devices
        .get_mut(device_id)
        .ok_or(HaliaError::NotFound)?
        .get_sink_tx(sink_id)
        .await
}

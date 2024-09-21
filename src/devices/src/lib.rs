use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    storage,
};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};
use tracing::warn;
use types::{
    devices::{
        CreateUpdateDeviceReq, DeviceConf, DeviceType, QueryParams, QueryRuleInfo,
        SearchDevicesItemCommon, SearchDevicesItemConf, SearchDevicesItemResp,
        SearchDevicesItemRunningInfo, SearchDevicesResp, SearchRuleInfo, Summary,
    },
    events::EventType,
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams, RuleRef,
    SearchSourcesOrSinksInfoResp, SearchSourcesOrSinksItemResp, SearchSourcesOrSinksResp, Value,
};

pub mod coap;
pub mod modbus;
pub mod opcua;

static GLOBAL_DEVICE_MANAGER: LazyLock<DashMap<String, Box<dyn Device>>> =
    LazyLock::new(|| DashMap::new());

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
    async fn read_running_info(&self) -> SearchDevicesItemRunningInfo;
    async fn update(&mut self, old_conf: String, new_conf: &serde_json::Value) -> HaliaResult<()>;
    async fn stop(&mut self) -> HaliaResult<()>;

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn update_source(
        &mut self,
        source_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn write_source_value(&mut self, source_id: String, req: Value) -> HaliaResult<()>;
    async fn delete_source(&mut self, source_id: &String) -> HaliaResult<()>;

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()>;
    async fn update_sink(
        &mut self,
        sink_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()>;

    async fn get_source_rx(
        &mut self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>>;

    async fn get_sink_tx(&mut self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>>;
}

pub async fn load_from_storage() -> HaliaResult<()> {
    let count = storage::device::count_all().await?;
    DEVICE_COUNT.store(count, Ordering::SeqCst);

    let db_devices = storage::device::read_on().await?;
    for db_device in db_devices {
        start_device(db_device.id).await?;
    }

    Ok(())
}

pub fn get_summary() -> Summary {
    let total = get_device_count();
    let on = get_device_on_count();
    let running = get_device_running_count();

    Summary { total, on, running }
}

pub async fn get_rule_info(query: QueryRuleInfo) -> HaliaResult<SearchRuleInfo> {
    let db_device = storage::device::read_device(&query.device_id).await?;

    let device_resp = transer_db_device_to_resp(db_device).await?;
    let id = match (query.source_id, query.sink_id) {
        (Some(source_id), None) => source_id,
        (None, Some(sink_id)) => sink_id,
        _ => {
            return Err(HaliaError::Common(
                "查询source_id或sink_id参数错误！".to_string(),
            ))
        }
    };

    let db_source_or_sink = storage::source_or_sink::read_one(&id).await?;
    Ok(SearchRuleInfo {
        device: device_resp,
        source: Some(SearchSourcesOrSinksInfoResp {
            id: db_source_or_sink.id,
            conf: CreateUpdateSourceOrSinkReq {
                base: BaseConf {
                    name: db_source_or_sink.name,
                    desc: db_source_or_sink
                        .des
                        .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                },
                ext: serde_json::from_slice(&db_source_or_sink.conf).unwrap(),
            },
        }),
        sink: None,
    })
}

pub async fn create_device(device_id: String, req: CreateUpdateDeviceReq) -> HaliaResult<()> {
    if storage::device::insert_name_exists(&req.conf.base.name).await? {
        return Err(HaliaError::NameExists);
    }

    match &req.typ {
        DeviceType::Modbus => modbus::validate_conf(&req.conf.ext)?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    }

    add_device_count();
    storage::device::insert(&device_id, req).await?;
    Ok(())
}

pub async fn search_devices(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<SearchDevicesResp> {
    // TODO 查询异常设备
    let (count, db_devices) = storage::device::search_devices(pagination, query_params).await?;
    let mut resp_devices = vec![];
    for db_device in db_devices {
        resp_devices.push(transer_db_device_to_resp(db_device).await?);
    }

    Ok(SearchDevicesResp {
        total: count,
        data: resp_devices,
    })
}

pub async fn update_device(device_id: String, req: CreateUpdateDeviceReq) -> HaliaResult<()> {
    if storage::device::update_name_exists(&req.conf.base.name, &device_id).await? {
        return Err(HaliaError::NameExists);
    }

    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let db_device = storage::device::read_device(&device_id).await?;
        device
            .update(
                unsafe { String::from_utf8_unchecked(db_device.conf) },
                &req.conf.ext,
            )
            .await?;
    }

    storage::device::update(&device_id, req).await?;

    Ok(())
}

pub async fn start_device(device_id: String) -> HaliaResult<()> {
    if GLOBAL_DEVICE_MANAGER.contains_key(&device_id) {
        return Ok(());
    }

    storage::event::insert(
        types::events::ResourceType::Device,
        &device_id,
        EventType::Start,
        None,
    )
    .await?;

    let db_device = storage::device::read_device(&device_id).await?;
    let typ = DeviceType::try_from(db_device.typ)?;

    let device_conf: DeviceConf = DeviceConf {
        base: BaseConf {
            name: db_device.name,
            desc: db_device
                .des
                .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
        },
        ext: serde_json::from_slice(&db_device.conf)?,
    };

    let mut device = match typ {
        DeviceType::Modbus => modbus::new(device_id.clone(), device_conf),
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    };

    let db_sources = storage::source_or_sink::read_all_by_parent_id(
        &device_id,
        storage::source_or_sink::Type::Source,
    )
    .await?;
    for db_source in db_sources {
        let conf: serde_json::Value = serde_json::from_slice(&db_source.conf).unwrap();
        device.create_source(db_source.id, conf).await?;
    }

    let db_sinks = storage::source_or_sink::read_all_by_parent_id(
        &device_id,
        storage::source_or_sink::Type::Sink,
    )
    .await?;
    for db_sink in db_sinks {
        let conf: serde_json::Value = serde_json::from_slice(&db_sink.conf).unwrap();
        device.create_sink(db_sink.id, conf).await?;
    }

    GLOBAL_DEVICE_MANAGER.insert(device_id.clone(), device);

    add_device_on_count();
    storage::device::update_status(&device_id, true).await?;
    Ok(())
}

pub async fn stop_device(device_id: String) -> HaliaResult<()> {
    // 设备已停止
    if !GLOBAL_DEVICE_MANAGER.contains_key(&device_id) {
        return Ok(());
    }

    let active_rule_ref_cnt = storage::rule_ref::count_active_cnt_by_parent_id(&device_id).await?;
    if active_rule_ref_cnt > 0 {
        return Err(HaliaError::StopActiveRefing);
    }

    if let Err(e) = storage::event::insert(
        types::events::ResourceType::Device,
        &device_id,
        types::events::EventType::Stop,
        None,
    )
    .await
    {
        warn!("create event failed: {}", e);
    }

    storage::device::update_status(&device_id, false).await?;
    GLOBAL_DEVICE_MANAGER
        .get_mut(&device_id)
        .unwrap()
        .stop()
        .await?;

    GLOBAL_DEVICE_MANAGER.remove(&device_id);
    sub_device_on_count();

    Ok(())
}

pub async fn delete_device(device_id: String) -> HaliaResult<()> {
    if GLOBAL_DEVICE_MANAGER.contains_key(&device_id) {
        return Err(HaliaError::Common("运行中，不能删除".to_string()));
    }

    let cnt = storage::rule_ref::count_cnt_by_parent_id(&device_id).await?;
    if cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    sub_device_count();
    storage::device::delete(&device_id).await?;
    storage::source_or_sink::delete_by_parent_id(&device_id).await?;

    Ok(())
}

pub async fn create_source(device_id: String, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
    if storage::source_or_sink::insert_name_exists(
        &device_id,
        storage::source_or_sink::Type::Source,
        &req.base.name,
    )
    .await?
    {
        return Err(HaliaError::NameExists);
    }

    let source_id = common::get_id();
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let conf = req.ext.clone();
        device.create_source(source_id.clone(), conf).await?;
    }

    storage::source_or_sink::insert(
        &device_id,
        &source_id,
        storage::source_or_sink::Type::Source,
        req,
    )
    .await?;

    Ok(())
}

pub async fn search_sources(
    device_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sources) = storage::source_or_sink::query_by_parent_id(
        &device_id,
        storage::source_or_sink::Type::Source,
        pagination,
        query,
    )
    .await?;
    let mut data = Vec::with_capacity(db_sources.len());
    for db_source in db_sources {
        let rule_ref = RuleRef {
            rule_ref_cnt: storage::rule_ref::count_cnt_by_resource_id(&db_source.id).await?,
            rule_active_ref_cnt: storage::rule_ref::count_active_cnt_by_resource_id(&db_source.id)
                .await?,
        };
        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id: db_source.id,
                conf: CreateUpdateSourceOrSinkReq {
                    base: BaseConf {
                        name: db_source.name,
                        desc: db_source
                            .des
                            .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                    },
                    ext: serde_json::from_slice(&db_source.conf).unwrap(),
                },
            },
            rule_ref,
        });
    }

    Ok(SearchSourcesOrSinksResp { total: count, data })
}

pub async fn update_source(
    device_id: String,
    source_id: String,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    if storage::source_or_sink::update_name_exists(
        &device_id,
        storage::source_or_sink::Type::Source,
        &req.base.name,
        &source_id,
    )
    .await?
    {
        return Err(HaliaError::NameExists);
    }

    let old_conf = storage::source_or_sink::read_conf(&source_id).await?;
    let new_conf = req.ext.clone();
    GLOBAL_DEVICE_MANAGER
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .update_source(&source_id, old_conf, new_conf)
        .await?;

    storage::source_or_sink::update(&source_id, req).await?;

    Ok(())
}

pub async fn write_source_value(
    device_id: String,
    source_id: String,
    req: Value,
) -> HaliaResult<()> {
    GLOBAL_DEVICE_MANAGER
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .write_source_value(source_id, req)
        .await
}

pub async fn delete_source(device_id: String, source_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule_ref::count_cnt_by_resource_id(&source_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    storage::source_or_sink::delete(&source_id).await?;

    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        device.delete_source(&source_id).await?;
    }

    Ok(())
}

pub async fn get_source_rx(
    device_id: &String,
    source_id: &String,
) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
    GLOBAL_DEVICE_MANAGER
        .get_mut(device_id)
        .ok_or(HaliaError::Stopped)?
        .get_source_rx(source_id)
        .await
}

pub async fn create_sink(device_id: String, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
    if storage::source_or_sink::insert_name_exists(
        &device_id,
        storage::source_or_sink::Type::Source,
        &req.base.name,
    )
    .await?
    {
        return Err(HaliaError::NameExists);
    }

    let sink_id = common::get_id();
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let conf: serde_json::Value = req.ext.clone();
        device.create_sink(sink_id.clone(), conf).await?;
    }

    storage::source_or_sink::insert(
        &device_id,
        &sink_id,
        storage::source_or_sink::Type::Sink,
        req,
    )
    .await?;

    Ok(())
}

pub async fn search_sinks(
    device_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sinks) = storage::source_or_sink::query_by_parent_id(
        &device_id,
        storage::source_or_sink::Type::Sink,
        pagination,
        query,
    )
    .await?;
    let mut data = Vec::with_capacity(db_sinks.len());
    for db_sink in db_sinks {
        let rule_ref = RuleRef {
            rule_ref_cnt: storage::rule_ref::count_cnt_by_resource_id(&db_sink.id).await?,
            rule_active_ref_cnt: storage::rule_ref::count_active_cnt_by_resource_id(&db_sink.id)
                .await?,
        };
        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id: db_sink.id,
                conf: CreateUpdateSourceOrSinkReq {
                    base: BaseConf {
                        name: db_sink.name,
                        desc: db_sink
                            .des
                            .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                    },
                    ext: serde_json::from_slice(&db_sink.conf).unwrap(),
                },
            },
            rule_ref,
        });
    }

    Ok(SearchSourcesOrSinksResp { total: count, data })
}

pub async fn update_sink(
    device_id: String,
    sink_id: String,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    if storage::source_or_sink::update_name_exists(
        &device_id,
        storage::source_or_sink::Type::Sink,
        &req.base.name,
        &sink_id,
    )
    .await?
    {
        return Err(HaliaError::NameExists);
    }

    let old_conf = storage::source_or_sink::read_conf(&sink_id).await?;
    let new_conf = req.ext.clone();
    GLOBAL_DEVICE_MANAGER
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound)?
        .update_sink(&sink_id, old_conf, new_conf)
        .await?;

    storage::source_or_sink::update(&sink_id, req).await?;

    Ok(())
}

pub async fn delete_sink(device_id: String, sink_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule_ref::count_cnt_by_resource_id(&sink_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        device.delete_sink(&sink_id).await?;
    }

    storage::source_or_sink::delete(&sink_id).await?;

    Ok(())
}

pub async fn get_sink_tx(
    device_id: &String,
    sink_id: &String,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    GLOBAL_DEVICE_MANAGER
        .get_mut(device_id)
        .ok_or(HaliaError::NotFound)?
        .get_sink_tx(sink_id)
        .await
}

async fn transer_db_device_to_resp(
    db_device: storage::device::Device,
) -> HaliaResult<SearchDevicesItemResp> {
    let source_cnt = storage::source_or_sink::count_by_parent_id(
        &db_device.id,
        storage::source_or_sink::Type::Source,
    )
    .await?;

    let sink_cnt = storage::source_or_sink::count_by_parent_id(
        &db_device.id,
        storage::source_or_sink::Type::Sink,
    )
    .await?;

    let running_info = match db_device.status {
        0 => None,
        1 => Some(
            GLOBAL_DEVICE_MANAGER
                .get(&db_device.id)
                .unwrap()
                .read_running_info()
                .await,
        ),
        _ => unreachable!(),
    };

    Ok(SearchDevicesItemResp {
        common: SearchDevicesItemCommon {
            id: db_device.id,
            typ: DeviceType::try_from(db_device.typ)?,
            on: db_device.status == 1,
            source_cnt,
            sink_cnt,
        },
        conf: SearchDevicesItemConf {
            base: BaseConf {
                name: db_device.name,
                desc: db_device
                    .des
                    .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
            },
            ext: serde_json::from_slice(&db_device.conf)?,
        },
        running_info,
    })
}

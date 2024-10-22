use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};
use types::{
    devices::{
        CreateUpdateDeviceReq, CreateUpdateSourceOrSinkReq, CreateUpdateSourceOrSinkTemplateReq,
        DeviceConf, DeviceType, QueryParams, QueryRuleInfo, QuerySourceOrSinkTemplateParams,
        SearchDevicesItemCommon, SearchDevicesItemConf, SearchDevicesItemResp,
        SearchDevicesItemRunningInfo, SearchDevicesResp, SearchRuleInfo,
        SearchSourcesOrSinkTemplatesItemResp, SearchSourcesOrSinkTemplatesResp,
        SearchSourcesOrSinksInfoResp, SearchSourcesOrSinksItemResp, SearchSourcesOrSinksResp,
        Summary,
    },
    BaseConf, Pagination, QuerySourcesOrSinksParams, RuleRef, Value,
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
    async fn update(
        &mut self,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn stop(&mut self);

    async fn create_customize_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn create_template_source(
        &mut self,
        source_id: String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()>;

    async fn update_customize_source(
        &mut self,
        source_id: &String,
        conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn update_template_source(
        &mut self,
        source_id: &String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn write_source_value(&mut self, source_id: String, req: Value) -> HaliaResult<()>;
    async fn delete_source(&mut self, source_id: &String) -> HaliaResult<()>;

    async fn create_customize_sink(
        &mut self,
        sink_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn create_template_sink(
        &mut self,
        sink_id: String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn update_sink(
        &mut self,
        sink_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()>;

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>>;

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>>;
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
    let db_device = storage::device::read_one(&query.device_id).await?;

    let device_resp = transer_db_device_to_resp(db_device).await?;
    match (query.source_id, query.sink_id) {
        (Some(source_id), None) => {
            let db_source = storage::device::source_sink::read_one(&source_id).await?;
            Ok(SearchRuleInfo {
                device: device_resp,
                source: Some(SearchSourcesOrSinksInfoResp {
                    id: db_source.id,
                    conf: CreateUpdateSourceOrSinkReq {
                        base: BaseConf {
                            name: db_source.name,
                            desc: db_source
                                .des
                                .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                        },
                        conf_type: db_source.conf_type.try_into()?,
                        ext: serde_json::from_slice(&db_source.conf).unwrap(),
                        // TODO
                        template_id: None,
                    },
                }),
                sink: None,
            })
        }
        (None, Some(sink_id)) => {
            let db_sink = storage::device::source_sink::read_one(&sink_id).await?;
            Ok(SearchRuleInfo {
                device: device_resp,
                source: None,
                sink: Some(SearchSourcesOrSinksInfoResp {
                    id: db_sink.id,
                    conf: CreateUpdateSourceOrSinkReq {
                        conf_type: db_sink.conf_type.try_into()?,
                        base: BaseConf {
                            name: db_sink.name,
                            desc: db_sink
                                .des
                                .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                        },
                        ext: serde_json::from_slice(&db_sink.conf).unwrap(),
                        // TODO
                        template_id: None,
                    },
                }),
            })
        }
        _ => Err(HaliaError::Common(
            "查询source_id或sink_id参数错误！".to_string(),
        )),
    }
}

pub async fn create_device(device_id: String, req: CreateUpdateDeviceReq) -> HaliaResult<()> {
    match &req.typ {
        DeviceType::Modbus => modbus::validate_conf(&req.conf.ext)?,
        DeviceType::Opcua => opcua::validate_conf(&req.conf.ext)?,
        DeviceType::Coap => coap::validate_conf(&req.conf.ext)?,
    }

    add_device_count();
    storage::device::insert(&device_id, req).await?;
    events::insert_create(types::events::ResourceType::Device, &device_id).await;

    Ok(())
}

pub async fn search_devices(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<SearchDevicesResp> {
    let (count, db_devices) = storage::device::search(pagination, query_params).await?;
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
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let old_conf = storage::device::read_conf(&device_id).await?;
        let old_conf: serde_json::Value = serde_json::from_slice(&old_conf)?;
        if old_conf == req.conf.ext {
            return Ok(());
        }

        device.update(old_conf, req.conf.ext.clone()).await?;
    }

    events::insert_update(types::events::ResourceType::Device, &device_id).await;
    storage::device::update_conf(&device_id, req).await?;

    Ok(())
}

pub async fn start_device(device_id: String) -> HaliaResult<()> {
    if GLOBAL_DEVICE_MANAGER.contains_key(&device_id) {
        return Ok(());
    }

    events::insert_start(types::events::ResourceType::Device, &device_id).await;

    let db_device = storage::device::read_one(&device_id).await?;
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
        DeviceType::Modbus => modbus::new(device_id.clone(), device_conf.ext.clone()),
        DeviceType::Opcua => opcua::new(device_id.clone(), device_conf.ext.clone()),
        DeviceType::Coap => coap::new(device_id.clone(), device_conf).await?,
    };

    let db_sources = storage::device::source_sink::read_sources_by_device_id(&device_id).await?;
    for db_source in db_sources {
        let conf_type: types::devices::SourceSinkConfType = db_source.conf_type.try_into()?;
        match conf_type {
            types::devices::SourceSinkConfType::Template => match db_source.template_id {
                Some(template_id) => {
                    let customize_conf: serde_json::Value =
                        serde_json::from_slice(&db_source.conf)?;
                    let template_conf =
                        storage::device::source_or_sink_template::read_conf(&template_id).await?;
                    let template_conf: serde_json::Value = serde_json::from_slice(&template_conf)?;
                    device
                        .create_template_source(db_source.id, customize_conf, template_conf)
                        .await?;
                }
                None => panic!("数据库数据损坏"),
            },
            types::devices::SourceSinkConfType::Customize => {
                let conf: serde_json::Value = serde_json::from_slice(&db_source.conf).unwrap();
                device.create_customize_source(db_source.id, conf).await?;
            }
        }
    }

    let db_sinks = storage::device::source_sink::read_sinks_by_device_id(&device_id).await?;
    for db_sink in db_sinks {
        let conf_type: types::devices::SourceSinkConfType = db_sink.conf_type.try_into()?;
        match conf_type {
            types::devices::SourceSinkConfType::Template => match db_sink.template_id {
                Some(template_id) => {
                    let customize_conf: serde_json::Value = serde_json::from_slice(&db_sink.conf)?;
                    let template_conf =
                        storage::device::source_or_sink_template::read_conf(&template_id).await?;
                    let template_conf: serde_json::Value = serde_json::from_slice(&template_conf)?;
                    device
                        .create_template_sink(db_sink.id, customize_conf, template_conf)
                        .await?;
                }
                None => panic!("数据库数据损坏"),
            },
            types::devices::SourceSinkConfType::Customize => {
                let conf: serde_json::Value = serde_json::from_slice(&db_sink.conf).unwrap();
                device.create_customize_sink(db_sink.id, conf).await?;
            }
        }
    }

    GLOBAL_DEVICE_MANAGER.insert(device_id.clone(), device);

    add_device_on_count();
    storage::device::update_status(&device_id, true).await?;
    Ok(())
}

pub async fn stop_device(device_id: String) -> HaliaResult<()> {
    if storage::rule::reference::count_active_cnt_by_parent_id(&device_id).await? > 0 {
        return Err(HaliaError::StopActiveRefing);
    }

    if let Some((_, mut device)) = GLOBAL_DEVICE_MANAGER.remove(&device_id) {
        device.stop().await;
        sub_device_on_count();
        events::insert_stop(types::events::ResourceType::Device, &device_id).await;
        storage::device::update_status(&device_id, false).await?;
        storage::device::update_err(&device_id, false).await?;
    }

    Ok(())
}

pub async fn delete_device(device_id: String) -> HaliaResult<()> {
    if GLOBAL_DEVICE_MANAGER.contains_key(&device_id) {
        return Err(HaliaError::Common("运行中，不能删除".to_string()));
    }

    let cnt = storage::rule::reference::count_cnt_by_parent_id(&device_id).await?;
    if cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    sub_device_count();
    events::insert_delete(types::events::ResourceType::Device, &device_id).await;
    storage::device::delete_by_id(&device_id).await?;
    storage::source_or_sink::delete_by_parent_id(&device_id).await?;

    Ok(())
}

pub async fn create_source(device_id: String, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
    if req.conf_type == types::devices::SourceSinkConfType::Template && req.template_id.is_none() {
        return Err(HaliaError::Common("模板ID不能为空".to_string()));
    }

    let typ: DeviceType = storage::device::read_type(&device_id).await?.try_into()?;
    match typ {
        DeviceType::Modbus => modbus::validate_source_conf(&req.ext)?,
        DeviceType::Opcua => opcua::validate_source_conf(&req.ext)?,
        DeviceType::Coap => coap::validate_source_conf(&req.ext)?,
    }

    let source_id = common::get_id();
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let conf = req.ext.clone();
        match req.conf_type {
            types::devices::SourceSinkConfType::Template => {
                let template_conf = storage::device::source_or_sink_template::read_conf(
                    // 函数入口处即进行了验证，此处永远不会panic
                    req.template_id.as_ref().unwrap(),
                )
                .await?;
                let template_conf: serde_json::Value = serde_json::from_slice(&template_conf)?;
                device
                    .create_template_source(source_id.clone(), conf, template_conf)
                    .await?
            }
            types::devices::SourceSinkConfType::Customize => {
                device
                    .create_customize_source(source_id.clone(), conf)
                    .await?
            }
        }
    }

    storage::device::source_sink::insert_source(&source_id, &device_id, req).await?;

    Ok(())
}

pub async fn search_sources(
    device_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sources) =
        storage::device::source_sink::search_sources(&device_id, pagination, query).await?;
    let mut data = Vec::with_capacity(db_sources.len());
    for db_source in db_sources {
        let rule_ref = RuleRef {
            rule_ref_cnt: storage::rule::reference::count_cnt_by_resource_id(&db_source.id).await?,
            rule_active_ref_cnt: storage::rule::reference::count_active_cnt_by_resource_id(
                &db_source.id,
            )
            .await?,
        };
        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id: db_source.id,
                conf: CreateUpdateSourceOrSinkReq {
                    conf_type: db_source.conf_type.try_into()?,
                    base: BaseConf {
                        name: db_source.name,
                        desc: db_source
                            .des
                            .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                    },
                    ext: serde_json::from_slice(&db_source.conf).unwrap(),
                    // TODO
                    template_id: None,
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
    if req.conf_type == types::devices::SourceSinkConfType::Template && req.template_id.is_none() {
        return Err(HaliaError::Common("模板ID不能为空".to_string()));
    }

    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        // let db_source = storage::device::source_sink::read_one(&source_id).await?;
        // let db_conf_type: types::devices::SourceSinkConfType = db_source.conf_type.try_into()?;
        match req.conf_type {
            types::devices::SourceSinkConfType::Template => {
                // TODO 判断配置相同的情况下，不更新
                let template_conf = storage::device::source_or_sink_template::read_conf(
                    req.template_id.as_ref().unwrap(),
                )
                .await?;
                let template_conf: serde_json::Value = serde_json::from_slice(&template_conf)?;
                device
                    .update_template_source(&source_id, req.ext.clone(), template_conf)
                    .await?;
            }
            types::devices::SourceSinkConfType::Customize => {
                device
                    .update_customize_source(&source_id, req.ext.clone())
                    .await?;
            }
        }
    }

    storage::device::source_sink::update(&source_id, req).await?;
    Ok(())
}

pub async fn write_source_value(
    device_id: String,
    source_id: String,
    req: Value,
) -> HaliaResult<()> {
    GLOBAL_DEVICE_MANAGER
        .get_mut(&device_id)
        .ok_or(HaliaError::NotFound(device_id))?
        .write_source_value(source_id, req)
        .await
}

pub async fn delete_source(device_id: String, source_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule::reference::count_cnt_by_resource_id(&source_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    storage::device::source_sink::delete_by_id(&source_id).await?;

    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        device.delete_source(&source_id).await?;
    }

    Ok(())
}

pub async fn get_source_rx(
    device_id: &String,
    source_id: &String,
) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
    if let Some(device) = GLOBAL_DEVICE_MANAGER.get(device_id) {
        device.get_source_rx(source_id).await
    } else {
        let device_name = storage::device::read_name(&device_id).await?;
        Err(HaliaError::Stopped(format!("设备：{}", device_name)))
    }
}

pub async fn create_sink(device_id: String, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
    if req.conf_type == types::devices::SourceSinkConfType::Template && req.template_id.is_none() {
        return Err(HaliaError::Common("模板ID不能为空".to_string()));
    }

    let typ = storage::device::read_type(&device_id).await?;
    let typ: DeviceType = typ.try_into()?;
    match typ {
        DeviceType::Modbus => modbus::validate_sink_conf(&req.ext)?,
        DeviceType::Opcua => opcua::validate_sink_conf(&req.ext)?,
        DeviceType::Coap => coap::validate_sink_conf(&req.ext)?,
    }

    let sink_id = common::get_id();
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let conf: serde_json::Value = req.ext.clone();
        match req.conf_type {
            types::devices::SourceSinkConfType::Template => {
                let template_conf = storage::device::source_or_sink_template::read_conf(
                    // 函数入口处即进行了验证，此处永远不会panic
                    req.template_id.as_ref().unwrap(),
                )
                .await?;
                let template_conf: serde_json::Value = serde_json::from_slice(&template_conf)?;
                device
                    .create_template_sink(sink_id.clone(), conf, template_conf)
                    .await?
            }
            types::devices::SourceSinkConfType::Customize => {
                device.create_customize_sink(sink_id.clone(), conf).await?
            }
        }
    }

    storage::device::source_sink::insert_sink(&device_id, &sink_id, req).await?;

    Ok(())
}

pub async fn search_sinks(
    device_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sinks) =
        storage::device::source_sink::search_sinks(&device_id, pagination, query).await?;
    let mut data = Vec::with_capacity(db_sinks.len());
    for db_sink in db_sinks {
        let rule_ref = RuleRef {
            rule_ref_cnt: storage::rule::reference::count_cnt_by_resource_id(&db_sink.id).await?,
            rule_active_ref_cnt: storage::rule::reference::count_active_cnt_by_resource_id(
                &db_sink.id,
            )
            .await?,
        };
        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id: db_sink.id,
                conf: CreateUpdateSourceOrSinkReq {
                    conf_type: db_sink.conf_type.try_into()?,
                    base: BaseConf {
                        name: db_sink.name,
                        desc: db_sink
                            .des
                            .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                    },
                    ext: serde_json::from_slice(&db_sink.conf).unwrap(),
                    // TODO
                    template_id: None,
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
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let old_conf = storage::source_or_sink::read_conf(&sink_id).await?;
        let new_conf = req.ext.clone();
        if old_conf != new_conf {
            device
                .update_sink(&sink_id, old_conf.clone(), new_conf.clone())
                .await?;
        }
    }

    storage::device::source_sink::update(&sink_id, req).await?;

    Ok(())
}

pub async fn delete_sink(device_id: String, sink_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule::reference::count_cnt_by_resource_id(&sink_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        device.delete_sink(&sink_id).await?;
    }

    storage::source_or_sink::delete_by_id(&sink_id).await?;

    Ok(())
}

pub async fn get_sink_tx(
    device_id: &String,
    sink_id: &String,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    if let Some(device) = GLOBAL_DEVICE_MANAGER.get(device_id) {
        device.get_sink_tx(sink_id).await
    } else {
        let device_name = storage::device::read_name(&device_id).await?;
        Err(HaliaError::Stopped(format!("设备：{}", device_name)))
    }
}

async fn transer_db_device_to_resp(
    db_device: storage::device::Device,
) -> HaliaResult<SearchDevicesItemResp> {
    let source_cnt =
        storage::device::source_sink::count_sources_by_device_id(&db_device.id).await?;
    let sink_cnt = storage::device::source_sink::count_sinks_by_device_id(&db_device.id).await?;

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

pub async fn create_source_template(req: CreateUpdateSourceOrSinkTemplateReq) -> HaliaResult<()> {
    match req.device_type {
        DeviceType::Modbus => modbus::source_template::validate(&req.ext)?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    }

    let id = common::get_id();
    storage::device::source_or_sink_template::insert(&id, storage::SourceSinkType::Source, req)
        .await?;

    Ok(())
}

pub async fn search_source_templates(
    pagination: Pagination,
    query: QuerySourceOrSinkTemplateParams,
) -> HaliaResult<SearchSourcesOrSinkTemplatesResp> {
    let (count, db_sources) = storage::device::source_or_sink_template::search(
        pagination,
        storage::SourceSinkType::Source,
        query,
    )
    .await?;

    let templates: Vec<_> = db_sources
        .into_iter()
        .map(|x| transer_db_source_sink_template_to_resp(x))
        .collect();

    Ok(SearchSourcesOrSinkTemplatesResp {
        total: count,
        data: templates,
    })
}

pub async fn update_source_template(
    id: String,
    req: CreateUpdateSourceOrSinkTemplateReq,
) -> HaliaResult<()> {
    // todo validate
    let old_conf = storage::device::source_or_sink_template::read_conf(&id).await?;
    let old_conf: serde_json::Value = serde_json::from_slice(&old_conf)?;
    if old_conf != req.ext {
        let sources = storage::device::source_sink::read_sources_by_template_id(&id).await?;
        for source in sources {
            if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&source.device_id) {
                let customize_conf: serde_json::Value = serde_json::from_slice(&source.conf)?;
                device
                    .update_template_source(&source.id, customize_conf, req.ext.clone())
                    .await?;
            }
        }
    }

    storage::device::source_or_sink_template::update_conf(&id, req).await?;
    Ok(())
}

pub async fn delete_source_template(id: String) -> HaliaResult<()> {
    if storage::device::source_or_sink_template_reference::count_by_template_id(&id).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }
    storage::device::source_or_sink_template_reference::delete_by_id(&id).await?;
    Ok(())
}

pub async fn create_sink_template(req: CreateUpdateSourceOrSinkTemplateReq) -> HaliaResult<()> {
    match req.device_type {
        DeviceType::Modbus => modbus::sink_template::validate(&req.ext)?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    }
    todo!()
}

fn transer_db_source_sink_template_to_resp(
    db_template: storage::device::source_or_sink_template::SourceSinkTemplate,
) -> SearchSourcesOrSinkTemplatesItemResp {
    SearchSourcesOrSinkTemplatesItemResp {
        id: db_template.id,
        name: db_template.name,
        desc: db_template
            .des
            .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
        device_type: db_template.device_type.try_into().unwrap(),
        conf: serde_json::from_slice(&db_template.conf).unwrap(),
    }
}

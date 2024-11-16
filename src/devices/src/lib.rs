use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use tokio::sync::mpsc;
use tracing::debug;
use types::{
    devices::{
        device::{
            self, source_sink, QueryParams, QueryRuleInfoParams, RunningInfo, SearchResp,
            SearchRuleInfo, SearchRuleSourceSinkResp,
        },
        ConfType, DeviceType, Summary,
    },
    Pagination, QuerySourcesOrSinksParams, RuleRef, Value,
};

pub mod coap;
pub mod device_template;
pub mod modbus;
pub mod opcua;
pub mod source_sink_template;

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
    async fn read_running_info(&self) -> RunningInfo;
    async fn update_customize_conf(&mut self, conf: serde_json::Value) -> HaliaResult<()>;
    async fn update_template_conf(
        &mut self,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
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
    async fn update_customize_sink(
        &mut self,
        sink_id: &String,
        conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn update_template_sink(
        &mut self,
        sink_id: &String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()>;

    async fn get_source_rxs(
        &self,
        source_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>>;

    async fn get_sink_txs(
        &self,
        sink_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>>;
}

pub async fn load_from_storage() -> HaliaResult<()> {
    let count = storage::device::device::count_all().await?;
    DEVICE_COUNT.store(count, Ordering::SeqCst);

    let db_devices = storage::device::device::read_many_on().await?;
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

pub async fn get_rule_info(query: QueryRuleInfoParams) -> HaliaResult<SearchRuleInfo> {
    let db_device = storage::device::device::read_one(&query.device_id).await?;

    let device_resp = transer_db_device_to_resp(db_device).await?;
    match (query.source_id, query.sink_id) {
        (Some(source_id), None) => {
            let db_source = storage::device::source_sink::read_one(&source_id).await?;
            Ok(SearchRuleInfo {
                device: device_resp,
                source: Some(SearchRuleSourceSinkResp {
                    id: source_id,
                    req: source_sink::CreateUpdateReq {
                        name: db_source.name,
                        conf_type: db_source.conf_type,
                        template_id: db_source.template_id,
                        conf: db_source.conf,
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
                sink: Some(SearchRuleSourceSinkResp {
                    id: db_sink.id,
                    req: source_sink::CreateUpdateReq {
                        name: db_sink.name,
                        conf_type: db_sink.conf_type,
                        template_id: db_sink.template_id,
                        conf: db_sink.conf,
                    },
                }),
            })
        }
        _ => Err(HaliaError::Common(
            "查询source_id或sink_id参数错误！".to_string(),
        )),
    }
}

pub async fn create_device(device_id: String, req: device::CreateReq) -> HaliaResult<()> {
    add_device_count();
    storage::device::device::insert(&device_id, req.clone()).await?;
    events::insert_create(types::events::ResourceType::Device, &device_id).await;

    match &req.conf_type {
        ConfType::Template => match &req.template_id {
            Some(device_template_id) => {
                if !storage::device::template::check_exists(&device_template_id).await? {
                    return Err(HaliaError::Common("模板不存在".to_owned()));
                }

                let device_template_sources =
                    storage::device::template_source_sink::read_sources_by_device_template_id(
                        device_template_id,
                    )
                    .await?;
                let mut source_reqs = vec![];
                for device_template_source in device_template_sources {
                    match device_template_source.conf_type {
                        ConfType::Template => {
                            let req = types::devices::device::source_sink::CreateUpdateReq {
                                name: device_template_source.name,
                                conf_type: device_template_source.conf_type,
                                template_id: device_template_source.template_id,
                                conf: device_template_source.conf,
                            };
                            source_reqs.push(req);
                        }
                        ConfType::Customize => {
                            let req = types::devices::device::source_sink::CreateUpdateReq {
                                name: device_template_source.name,
                                conf_type: device_template_source.conf_type,
                                template_id: None,
                                conf: device_template_source.conf,
                            };
                            source_reqs.push(req);
                        }
                    }
                }

                let device_template_sinks =
                    storage::device::template_source_sink::read_sinks_by_device_template_id(
                        device_template_id,
                    )
                    .await?;
                let mut sink_reqs = vec![];
                for device_template_sink in device_template_sinks {
                    match device_template_sink.conf_type {
                        ConfType::Template => {
                            let req = types::devices::device::source_sink::CreateUpdateReq {
                                name: device_template_sink.name,
                                conf_type: device_template_sink.conf_type,
                                template_id: device_template_sink.template_id,
                                conf: device_template_sink.conf,
                            };
                            sink_reqs.push(req);
                        }
                        ConfType::Customize => {
                            let req = types::devices::device::source_sink::CreateUpdateReq {
                                name: device_template_sink.name,
                                conf_type: device_template_sink.conf_type,
                                template_id: None,
                                conf: device_template_sink.conf,
                            };
                            sink_reqs.push(req);
                        }
                    }
                }

                for source_req in source_reqs {
                    create_source(device_id.clone(), Some(&device_template_id), source_req).await?;
                }

                for sink_req in sink_reqs {
                    create_sink(device_id.clone(), Some(&device_template_id), sink_req).await?;
                }
            }
            None => return Err(HaliaError::Common("必须提供模板ID".to_owned())),
        },
        ConfType::Customize => match &req.device_type {
            DeviceType::Modbus => modbus::validate_conf(&req.conf)?,
            DeviceType::Opcua => opcua::validate_conf(&req.conf)?,
            DeviceType::Coap => coap::validate_conf(&req.conf)?,
        },
    }

    Ok(())
}

pub async fn list_devices(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<SearchResp> {
    let (count, db_devices) = storage::device::device::search(pagination, query_params).await?;
    let mut resp_devices = vec![];
    for db_device in db_devices {
        resp_devices.push(transer_db_device_to_resp(db_device).await?);
    }

    Ok(SearchResp {
        total: count,
        data: resp_devices,
    })
}

pub async fn update_device(device_id: String, req: device::UpdateReq) -> HaliaResult<()> {
    match &req.conf_type {
        ConfType::Template => match &req.template_id {
            Some(template_id) => {
                let template_conf = storage::device::template::read_conf(&template_id).await?;
                let template_conf: serde_json::Value = serde_json::from_slice(&template_conf)?;
                if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
                    device
                        .update_template_conf(req.conf.clone(), template_conf)
                        .await?;
                }
            }
            None => return Err(HaliaError::Common("模板ID不能为空".to_string())),
        },
        ConfType::Customize => {
            if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
                device.update_customize_conf(req.conf.clone()).await?;
            }
        }
    }

    events::insert_update(types::events::ResourceType::Device, &device_id).await;
    storage::device::device::update_conf(&device_id, req).await?;

    Ok(())
}

pub async fn start_device(device_id: String) -> HaliaResult<()> {
    if GLOBAL_DEVICE_MANAGER.contains_key(&device_id) {
        return Ok(());
    }

    events::insert_start(types::events::ResourceType::Device, &device_id).await;

    let db_device = storage::device::device::read_one(&device_id).await?;
    let mut device = match db_device.conf_type {
        ConfType::Template => match db_device.template_id {
            Some(template_id) => {
                let template_conf = storage::device::template::read_conf(&template_id).await?;
                let template_conf: serde_json::Value = serde_json::from_slice(&template_conf)?;
                match db_device.device_type {
                    DeviceType::Modbus => modbus::new_by_template_conf(
                        db_device.id.clone(),
                        db_device.conf,
                        template_conf,
                    ),
                    DeviceType::Opcua => todo!(),
                    DeviceType::Coap => todo!(),
                }
            }
            None => unreachable!(),
        },
        ConfType::Customize => match db_device.device_type {
            DeviceType::Modbus => {
                modbus::new_by_customize_conf(db_device.id.clone(), db_device.conf)
            }
            DeviceType::Opcua => todo!(),
            DeviceType::Coap => todo!(),
        },
    };

    let db_sources = storage::device::source_sink::read_sources_by_device_id(&device_id).await?;
    for db_source in db_sources {
        match db_source.conf_type {
            types::devices::ConfType::Template => match db_source.template_id {
                Some(template_id) => {
                    let template_conf =
                        storage::device::source_sink_template::read_conf(&template_id).await?;
                    device
                        .create_template_source(db_source.id, db_source.conf, template_conf)
                        .await?;
                }
                None => panic!("数据库数据损坏"),
            },
            types::devices::ConfType::Customize => {
                device
                    .create_customize_source(db_source.id, db_source.conf)
                    .await?;
            }
        }
    }

    let db_sinks = storage::device::source_sink::read_sinks_by_device_id(&device_id).await?;
    for db_sink in db_sinks {
        match db_sink.conf_type {
            types::devices::ConfType::Template => match db_sink.template_id {
                Some(template_id) => {
                    let template_conf =
                        storage::device::source_sink_template::read_conf(&template_id).await?;
                    device
                        .create_template_sink(db_sink.id, db_sink.conf, template_conf)
                        .await?;
                }
                None => panic!("数据库数据损坏"),
            },
            types::devices::ConfType::Customize => {
                device
                    .create_customize_sink(db_sink.id, db_sink.conf)
                    .await?;
            }
        }
    }

    GLOBAL_DEVICE_MANAGER.insert(device_id.clone(), device);

    add_device_on_count();
    storage::device::device::update_status(&device_id, types::Boolean::True).await?;
    storage::device::device::update_err(&device_id, types::Boolean::False).await?;
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
        storage::device::device::update_status(&device_id, types::Boolean::False).await?;
        storage::device::device::update_err(&device_id, types::Boolean::False).await?;
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
    storage::device::device::delete_by_id(&device_id).await?;
    storage::device::source_sink::delete_many_by_device_id(&device_id).await?;

    Ok(())
}

pub async fn device_create_source(
    device_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    let conf_type = storage::device::device::read_conf_type(&device_id).await?;
    if conf_type == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能创建源".to_string()));
    }
    create_source(device_id, None, req).await
}

pub(crate) async fn create_source(
    device_id: String,
    device_template_source_id: Option<&String>,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    if req.conf_type == ConfType::Template && req.template_id.is_none() {
        return Err(HaliaError::Common("模板ID不能为空".to_string()));
    }

    let device_type: DeviceType = storage::device::device::read_device_type(&device_id)
        .await?
        .try_into()?;
    match device_type {
        DeviceType::Modbus => modbus::validate_source_conf(&req.conf)?,
        DeviceType::Opcua => opcua::validate_source_conf(&req.conf)?,
        DeviceType::Coap => coap::validate_source_conf(&req.conf)?,
    }

    let source_id = common::get_id();
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let conf = req.conf.clone();
        match req.conf_type {
            types::devices::ConfType::Template => {
                let template_conf = storage::device::source_sink_template::read_conf(
                    // 函数入口处即进行了验证，此处永远不会panic
                    req.template_id.as_ref().unwrap(),
                )
                .await?;
                device
                    .create_template_source(source_id.clone(), conf, template_conf)
                    .await?
            }
            types::devices::ConfType::Customize => {
                device
                    .create_customize_source(source_id.clone(), conf)
                    .await?
            }
        }
    }

    storage::device::source_sink::insert_source(
        &source_id,
        &device_id,
        device_template_source_id,
        req,
    )
    .await?;

    Ok(())
}

pub async fn search_sources(
    device_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<source_sink::SearchResp> {
    let (count, db_sources) =
        storage::device::source_sink::search_sources(&device_id, pagination, query).await?;
    debug!("here");
    let mut data = Vec::with_capacity(db_sources.len());
    for db_source in db_sources {
        let rule_ref = RuleRef {
            rule_ref_cnt: storage::rule::reference::count_cnt_by_resource_id(&db_source.id).await?,
            rule_active_ref_cnt: storage::rule::reference::count_active_cnt_by_resource_id(
                &db_source.id,
            )
            .await?,
        };
        data.push(source_sink::SearchItemResp {
            id: db_source.id,
            rule_ref,
            req: source_sink::CreateUpdateReq {
                name: db_source.name,
                conf_type: db_source.conf_type,
                template_id: db_source.template_id,
                conf: db_source.conf,
            },
        });
    }

    Ok(source_sink::SearchResp { total: count, data })
}

pub async fn update_source(
    device_id: String,
    source_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    let device_conf_type = storage::device::device::read_conf_type(&device_id).await?;
    if device_conf_type == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能修改源。".to_string()));
    }

    if req.conf_type == types::devices::ConfType::Template && req.template_id.is_none() {
        return Err(HaliaError::Common("模板ID不能为空".to_string()));
    }

    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        // let db_source = storage::device::source_sink::read_one(&source_id).await?;
        // let db_conf_type: types::devices::SourceSinkConfType = db_source.conf_type.try_into()?;
        match req.conf_type {
            types::devices::ConfType::Template => {
                // TODO 判断配置相同的情况下，不更新
                let template_conf = storage::device::source_sink_template::read_conf(
                    req.template_id.as_ref().unwrap(),
                )
                .await?;
                device
                    .update_template_source(&source_id, req.conf.clone(), template_conf)
                    .await?;
            }
            types::devices::ConfType::Customize => {
                device
                    .update_customize_source(&source_id, req.conf.clone())
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

pub async fn device_delete_source(device_id: String, source_id: String) -> HaliaResult<()> {
    if storage::device::device::read_conf_type(&device_id).await? == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能删除源".to_string()));
    }
    delete_source(device_id, source_id).await
}

pub(crate) async fn delete_source(device_id: String, source_id: String) -> HaliaResult<()> {
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

pub async fn get_source_rxs(
    device_id: &String,
    source_id: &String,
    cnt: usize,
) -> HaliaResult<Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>> {
    if let Some(device) = GLOBAL_DEVICE_MANAGER.get(device_id) {
        device.get_source_rxs(source_id, cnt).await
    } else {
        let device_name = storage::device::device::read_name(&device_id).await?;
        Err(HaliaError::Stopped(format!("设备：{}", device_name)))
    }
}

pub async fn device_create_sink(
    device_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    if storage::device::device::read_conf_type(&device_id).await? == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能创建动作。".to_string()));
    }
    create_sink(device_id, None, req).await
}

pub(crate) async fn create_sink(
    device_id: String,
    device_template_sink_id: Option<&String>,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    if req.conf_type == types::devices::ConfType::Template && req.template_id.is_none() {
        return Err(HaliaError::Common("模板ID不能为空".to_string()));
    }

    let device_type: DeviceType = storage::device::device::read_device_type(&device_id)
        .await?
        .try_into()?;
    match device_type {
        DeviceType::Modbus => modbus::validate_sink_conf(&req.conf)?,
        DeviceType::Opcua => opcua::validate_sink_conf(&req.conf)?,
        DeviceType::Coap => coap::validate_sink_conf(&req.conf)?,
    }

    let sink_id = common::get_id();
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let conf: serde_json::Value = req.conf.clone();
        match req.conf_type {
            types::devices::ConfType::Template => {
                let template_conf = storage::device::source_sink_template::read_conf(
                    // 函数入口处即进行了验证，此处永远不会panic
                    req.template_id.as_ref().unwrap(),
                )
                .await?;
                device
                    .create_template_sink(sink_id.clone(), conf, template_conf)
                    .await?
            }
            types::devices::ConfType::Customize => {
                device.create_customize_sink(sink_id.clone(), conf).await?
            }
        }
    }

    storage::device::source_sink::insert_sink(&sink_id, &device_id, device_template_sink_id, req)
        .await?;

    Ok(())
}

pub async fn search_sinks(
    device_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<source_sink::SearchResp> {
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
        data.push(source_sink::SearchItemResp {
            id: db_sink.id,
            req: source_sink::CreateUpdateReq {
                name: db_sink.name,
                conf_type: db_sink.conf_type,
                template_id: db_sink.template_id,
                conf: db_sink.conf,
            },
            rule_ref,
        });
    }

    Ok(source_sink::SearchResp { total: count, data })
}

pub async fn update_sink(
    device_id: String,
    sink_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    if req.conf_type == ConfType::Template && req.template_id.is_none() {
        return Err(HaliaError::Common("模板ID不能为空".to_string()));
    }

    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        match req.conf_type {
            ConfType::Template => {
                let template_conf = storage::device::source_sink_template::read_conf(
                    req.template_id.as_ref().unwrap(),
                )
                .await?;
                device
                    .update_template_sink(&sink_id, req.conf.clone(), template_conf)
                    .await?;
            }
            ConfType::Customize => {
                device
                    .update_customize_sink(&sink_id, req.conf.clone())
                    .await?;
            }
        }
    }

    storage::device::source_sink::update(&sink_id, req).await?;

    Ok(())
}

pub async fn device_delete_sink(device_id: String, sink_id: String) -> HaliaResult<()> {
    if storage::device::device::read_conf_type(&device_id).await? == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能删除源".to_string()));
    }
    delete_sink(device_id, sink_id).await
}

pub(crate) async fn delete_sink(device_id: String, sink_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule::reference::count_cnt_by_resource_id(&sink_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        device.delete_sink(&sink_id).await?;
    }

    storage::device::source_sink::delete_by_id(&sink_id).await?;

    Ok(())
}

pub async fn get_sink_txs(
    device_id: &String,
    sink_id: &String,
    cnt: usize,
) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>> {
    if let Some(device) = GLOBAL_DEVICE_MANAGER.get(device_id) {
        device.get_sink_txs(sink_id, cnt).await
    } else {
        let device_name = storage::device::device::read_name(&device_id).await?;
        Err(HaliaError::Stopped(format!("设备：{}", device_name)))
    }
}

async fn transer_db_device_to_resp(
    db_device: storage::device::device::Device,
) -> HaliaResult<device::SearchItemResp> {
    let source_cnt =
        storage::device::source_sink::count_sources_by_device_id(&db_device.id).await?;
    let sink_cnt = storage::device::source_sink::count_sinks_by_device_id(&db_device.id).await?;

    let running_info = match db_device.status {
        types::Boolean::True => Some(
            GLOBAL_DEVICE_MANAGER
                .get(&db_device.id)
                .unwrap()
                .read_running_info()
                .await,
        ),
        types::Boolean::False => None,
    };

    Ok(device::SearchItemResp {
        id: db_device.id,
        req: device::CreateReq {
            name: db_device.name,
            device_type: db_device.device_type,
            conf_type: db_device.conf_type,
            template_id: db_device.template_id,
            conf: db_device.conf,
        },
        running_info,
        source_cnt,
        sink_cnt,
    })
}

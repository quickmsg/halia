use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use tokio::sync::mpsc;
use tracing::debug;
use types::{
    devices::{
        device::QueryParams, ConfType, DeviceSourceGroupCreateReq, DeviceSourceGroupListItem,
        DeviceSourceGroupListResp, DeviceSourceGroupQueryParams, DeviceSourceGroupReadResp,
        DeviceSourceGroupUpdateReq, DeviceType, ListDevicesResp, ListSourcesSinksItem,
        ListSourcesSinksResp, QueryRuleInfoParams, ReadDeviceResp, ReadSourceSinkResp,
        RuleInfoDevice, RuleInfoResp, RuleInfoSourceSink, SourceSinkCreateUpdateReq,
        SourceSinkQueryParams,
    },
    Pagination, Status, Summary, Value,
};

pub mod coap;
pub mod device_template;
pub mod modbus;
pub mod opcua;
pub mod source_group;

static GLOBAL_DEVICE_MANAGER: LazyLock<DashMap<String, Box<dyn Device>>> =
    LazyLock::new(|| DashMap::new());

#[async_trait]
pub(crate) trait Device: Send + Sync {
    async fn read_device_err(&self) -> Option<Arc<String>>;
    async fn read_source_err(&self, source_id: &String) -> Option<String>;
    async fn read_sink_err(&self, sink_id: &String) -> Option<String>;
    async fn update_conf(
        &mut self,
        mode: UpdateConfMode,
        customize_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn stop(&mut self);

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn update_source(
        &mut self,
        source_id: &String,
        conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn write_source_value(&mut self, _source_id: String, _req: Value) -> HaliaResult<()> {
        return Err(HaliaError::Common("不支持写入数据。".into()));
    }
    async fn delete_source(&mut self, source_id: &String) -> HaliaResult<()>;

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()>;
    async fn update_sink(&mut self, sink_id: &String, conf: serde_json::Value) -> HaliaResult<()>;
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
    let db_devices = storage::device::device::read_many_on().await?;
    for db_device in db_devices {
        start_device(db_device.id).await?;
    }

    Ok(())
}

pub async fn get_summary() -> HaliaResult<Summary> {
    let (total, running_cnt, error_cnt) = storage::device::device::get_summary().await?;
    Ok(Summary {
        total,
        running_cnt,
        error_cnt: Some(error_cnt),
    })
}

// 规则中读取详情
pub async fn get_rule_info(query: QueryRuleInfoParams) -> HaliaResult<RuleInfoResp> {
    let db_device = storage::device::device::read_one(&query.device_id).await?;
    let device = RuleInfoDevice {
        id: db_device.id,
        name: db_device.name,
        status: db_device.status,
    };
    match (query.source_id, query.sink_id) {
        (Some(source_id), None) => {
            let db_source = storage::device::source_sink::read_one(&source_id).await?;
            Ok(RuleInfoResp {
                device,
                source: Some(RuleInfoSourceSink {
                    id: db_source.id,
                    name: db_source.name,
                    status: db_source.status,
                }),
                sink: None,
            })
        }
        (None, Some(sink_id)) => {
            let db_sink: storage::device::source_sink::SourceSink =
                storage::device::source_sink::read_one(&sink_id).await?;
            Ok(RuleInfoResp {
                device,
                source: None,
                sink: Some(RuleInfoSourceSink {
                    id: db_sink.id,
                    name: db_sink.name,
                    status: db_sink.status,
                }),
            })
        }
        _ => Err(HaliaError::Common(
            "查询source_id或sink_id参数错误！".to_string(),
        )),
    }
}

pub async fn create_device(
    device_id: String,
    req: types::devices::device::CreateReq,
) -> HaliaResult<()> {
    match &req.conf_type {
        ConfType::Template => match &req.template_id {
            Some(device_template_id) => {
                match &req.device_type {
                    DeviceType::Modbus => {
                        modbus::template::validate_device_customize_conf(req.conf.clone())?
                    }
                    DeviceType::Opcua => todo!(),
                    DeviceType::Coap => todo!(),
                }
                let device_template_sources =
                    storage::device::template_source_sink::read_sources_by_device_template_id(
                        device_template_id,
                    )
                    .await?;
                let mut source_reqs = vec![];
                for device_template_source in device_template_sources {
                    let req = types::devices::SourceSinkCreateUpdateReq {
                        name: device_template_source.name,
                        conf: device_template_source.conf,
                    };
                    source_reqs.push(req);
                }

                let device_template_sinks =
                    storage::device::template_source_sink::read_sinks_by_device_template_id(
                        device_template_id,
                    )
                    .await?;
                let mut sink_reqs = vec![];
                for device_template_sink in device_template_sinks {
                    let req = types::devices::SourceSinkCreateUpdateReq {
                        name: device_template_sink.name,
                        conf: device_template_sink.conf,
                    };
                    sink_reqs.push(req);
                }

                // 从模板中进行初始化

                // for source_req in source_reqs {
                //     create_source(device_id.clone(), Some(&device_template_id), source_req).await?;
                // }

                // for sink_req in sink_reqs {
                //     create_sink(device_id.clone(), Some(&device_template_id), sink_req).await?;
                // }
            }
            None => return Err(HaliaError::Common("必须提供模板ID".to_owned())),
        },
        ConfType::Customize => match &req.device_type {
            DeviceType::Modbus => modbus::validate_conf(&req.conf)?,
            DeviceType::Opcua => opcua::validate_conf(&req.conf)?,
            DeviceType::Coap => coap::validate_conf(&req.conf)?,
        },
    }

    storage::device::device::insert(&device_id, req.clone()).await?;
    events::insert_create(types::events::ResourceType::Device, &device_id).await;

    Ok(())
}

pub async fn list_devices(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<ListDevicesResp> {
    let (count, db_devices) = storage::device::device::search(pagination, query_params).await?;
    // let xx = stream::iter(db_devices)
    //     .then(|db_device| async move {
    //         let source_cnt =
    //             storage::device::source_sink::count_sources_by_device_id(&db_device.id).await?;
    //         let sink_cnt =
    //             storage::device::source_sink::count_sinks_by_device_id(&db_device.id).await?;
    //         let rule_ref_cnt =
    //             storage::rule::reference::get_rule_ref_info_by_parent_id(&db_device.id).await?;
    //         let err = match db_device.status {
    //             Status::Error => {
    //                 let device = match GLOBAL_DEVICE_MANAGER.get(&db_device.id) {
    //                     Some(device) => device,
    //                     None => return Err(HaliaError::Common("设备未启动！".to_string())),
    //                 };
    //                 device.read_err().await
    //             }
    //             _ => None,
    //         };

    //         Ok(types::devices::ListDevicesItem {
    //             id: db_device.id,
    //             name: db_device.name,
    //             device_type: db_device.device_type,
    //             status: db_device.status,
    //             err,
    //             rule_ref_cnt,
    //             source_cnt,
    //             sink_cnt,
    //         })
    //     })
    //     .collect::<HaliaResult<Vec<_>>>()
    //     .await;

    let mut list = Vec::with_capacity(db_devices.len());
    for db_device in db_devices {
        let source_cnt =
            storage::device::source_sink::count_sources_by_device_id(&db_device.id).await?;
        let sink_cnt =
            storage::device::source_sink::count_sinks_by_device_id(&db_device.id).await?;
        let rule_ref_cnt =
            storage::rule::reference::get_rule_ref_info_by_parent_id(&db_device.id).await?;
        let err = match db_device.status {
            Status::Error => {
                let device = match GLOBAL_DEVICE_MANAGER.get(&db_device.id) {
                    Some(device) => device,
                    None => return Err(HaliaError::Common("设备未启动！".to_string())),
                };
                device.read_device_err().await
            }
            _ => None,
        };

        let addr = match &db_device.device_type {
            DeviceType::Modbus => match &db_device.conf_type {
                ConfType::Template => {
                    let customize_conf: types::devices::device_template::modbus::CustomizeConf =
                        serde_json::from_value(db_device.conf)?;
                    match (customize_conf.ethernet, customize_conf.serial) {
                        (None, Some(serial)) => serial.path,
                        (Some(ethernet), None) => {
                            format!("tcp://{}:{}", ethernet.host, ethernet.port)
                        }
                        _ => unreachable!(),
                    }
                }
                ConfType::Customize => {
                    let conf: types::devices::device::modbus::DeviceConf =
                        serde_json::from_value(db_device.conf)?;
                    match (conf.ethernet, conf.serial) {
                        (None, Some(serial)) => serial.path,
                        (Some(ethernet), None) => {
                            format!("tcp://{}:{}", ethernet.host, ethernet.port)
                        }
                        _ => unreachable!(),
                    }
                }
            },
            DeviceType::Opcua => todo!(),
            DeviceType::Coap => todo!(),
        };

        let device = types::devices::ListDevicesItem {
            id: db_device.id,
            name: db_device.name,
            device_type: db_device.device_type,
            conf_type: db_device.conf_type,
            status: db_device.status,
            err,
            rule_ref_cnt,
            source_cnt,
            sink_cnt,
            addr,
        };
        list.push(device);
    }

    Ok(ListDevicesResp { count, list })
}

pub async fn read_device(device_id: String) -> HaliaResult<types::devices::ReadDeviceResp> {
    let db_device = storage::device::device::read_one(&device_id).await?;
    let err = match db_device.status {
        Status::Error => {
            let device = match GLOBAL_DEVICE_MANAGER.get(&device_id) {
                Some(device) => device,
                None => return Err(HaliaError::Common("设备未启动！".to_string())),
            };
            device.read_device_err().await
        }
        _ => None,
    };
    let template_conf = match &db_device.conf_type {
        ConfType::Template => {
            let template_id = db_device.template_id.as_ref().unwrap();
            let template_conf = storage::device::template::read_conf(template_id).await?;
            Some(template_conf)
        }
        ConfType::Customize => None,
    };
    Ok(ReadDeviceResp {
        id: db_device.id,
        device_type: db_device.device_type,
        conf_type: db_device.conf_type,
        template_id: db_device.template_id,
        name: db_device.name,
        conf: db_device.conf,
        template_conf,
        status: db_device.status,
        err,
    })
}

pub async fn update_device(
    device_id: String,
    req: types::devices::device::UpdateReq,
) -> HaliaResult<()> {
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        let db_device = storage::device::device::read_one(&device_id).await?;
        let mode = match db_device.conf_type {
            ConfType::Customize => UpdateConfMode::CustomizeMode,
            ConfType::Template => UpdateConfMode::TemplateModeCustomize,
        };
        device.update_conf(mode, req.conf.clone()).await?;
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
                match db_device.device_type {
                    DeviceType::Modbus => {
                        modbus::new_by_template(db_device.id.clone(), db_device.conf, template_conf)
                    }
                    DeviceType::Opcua => todo!(),
                    DeviceType::Coap => todo!(),
                }
            }
            None => unreachable!(),
        },
        ConfType::Customize => match db_device.device_type {
            DeviceType::Modbus => modbus::new_by_customize(db_device.id.clone(), db_device.conf),
            DeviceType::Opcua => todo!(),
            DeviceType::Coap => todo!(),
        },
    };

    let db_sources = storage::device::source_sink::read_sources_by_device_id(&device_id).await?;
    for db_source in db_sources {
        // match db_source.device_template_source_sink_id {
        //     Some(device_template_source_sink_id) => {
        //         let db_template_source_sink = storage::device::template_source_sink::read_one(
        //             &device_template_source_sink_id,
        //         )
        //         .await?;

        //         device
        //             .create_source(db_source.id, db_template_source_sink.conf)
        //             .await?;
        //     }
        //     None => {
        //         device.create_source(db_source.id, db_source.conf).await?;
        //     }
        // }
    }

    let db_sinks = storage::device::source_sink::read_sinks_by_device_id(&device_id).await?;
    for db_sink in db_sinks {
        // match db_sink.device_template_source_sink_id {
        //     Some(device_template_source_sink_id) => {
        //         let db_template_sink = storage::device::template_source_sink::read_one(
        //             &device_template_source_sink_id,
        //         )
        //         .await?;

        //         device
        //             .create_sink(db_sink.id, db_template_sink.conf)
        //             .await?;
        //     }
        //     None => {
        //         device.create_sink(db_sink.id, db_sink.conf).await?;
        //     }
        // }
    }

    GLOBAL_DEVICE_MANAGER.insert(device_id.clone(), device);

    storage::device::device::update_status(&device_id, types::Status::Running).await?;
    storage::device::source_sink::update_status_by_device_id(&device_id, types::Status::Running)
        .await?;

    Ok(())
}

pub async fn stop_device(device_id: String) -> HaliaResult<()> {
    if storage::rule::reference::count_cnt_by_parent_id(&device_id, Some(Status::Running)).await?
        > 0
    {
        return Err(HaliaError::StopActiveRefing);
    }

    if let Some((_, mut device)) = GLOBAL_DEVICE_MANAGER.remove(&device_id) {
        device.stop().await;
        events::insert_stop(types::events::ResourceType::Device, &device_id).await;
        storage::device::device::update_status(&device_id, types::Status::Stopped).await?;
        storage::device::source_sink::update_status_by_device_id(
            &device_id,
            types::Status::Stopped,
        )
        .await?;
    }

    Ok(())
}

pub async fn delete_device(device_id: String) -> HaliaResult<()> {
    if GLOBAL_DEVICE_MANAGER.contains_key(&device_id) {
        return Err(HaliaError::Common("运行中，不能删除".to_string()));
    }

    if storage::rule::reference::count_cnt_by_parent_id(&device_id, None).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    events::insert_delete(types::events::ResourceType::Device, &device_id).await;
    storage::device::source_sink::delete_many_by_device_id(&device_id).await?;
    storage::device::device::delete_by_id(&device_id).await?;

    Ok(())
}

pub async fn device_create_source(
    device_id: String,
    req: SourceSinkCreateUpdateReq,
) -> HaliaResult<()> {
    let conf_type = storage::device::device::read_conf_type(&device_id).await?;
    if conf_type == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能创建源".to_string()));
    }

    let source_id = create_source(&device_id, req.conf.clone()).await?;
    storage::device::source_sink::device_insert_source(&source_id, &device_id, req).await?;
    Ok(())
}

pub(crate) async fn device_template_create_source(
    device_id: String,
    device_template_source_id: &String,
    name: String,
    conf: serde_json::Value,
) -> HaliaResult<()> {
    let source_id = create_source(&device_id, conf).await?;
    storage::device::source_sink::device_template_insert_source(
        &source_id,
        &device_id,
        &name,
        &device_template_source_id,
    )
    .await?;

    Ok(())
}

pub(crate) async fn source_group_create_source(
    device_id: &String,
    source_group_source_id: &String,
    name: String,
    conf: serde_json::Value,
) -> HaliaResult<()> {
    let source_id = create_source(&device_id, conf).await?;
    storage::device::source_sink::source_group_insert_source(
        &source_id,
        device_id,
        &name,
        source_group_source_id,
    )
    .await?;
    Ok(())
}

async fn create_source(device_id: &String, conf: serde_json::Value) -> HaliaResult<String> {
    let source_id = common::get_id();
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(device_id) {
        device.create_source(source_id.clone(), conf).await?
    }

    Ok(source_id)
}

pub async fn list_sources(
    device_id: String,
    pagination: Pagination,
    query: SourceSinkQueryParams,
) -> HaliaResult<ListSourcesSinksResp> {
    let device_type = storage::device::device::read_device_type(&device_id).await?;
    let (count, db_sources) =
        storage::device::source_sink::search_sources(&device_id, pagination, query).await?;
    let mut list = Vec::with_capacity(db_sources.len());
    for db_source in db_sources {
        let rule_ref_cnt =
            storage::rule::reference::get_rule_ref_info_by_resource_id(&db_source.id).await?;
        let err = match db_source.status {
            Status::Error => match GLOBAL_DEVICE_MANAGER.get(&device_id) {
                Some(device) => device.read_source_err(&db_source.id).await,
                None => Some("应用未启动！".to_string()),
            },
            _ => None,
        };
        let conf = get_source_conf_from_db(&device_type, &db_source).await?;
        list.push(ListSourcesSinksItem {
            id: db_source.id,
            name: db_source.name,
            status: db_source.status,
            source_from_type: Some(db_source.source_from_type),
            err,
            rule_ref_cnt,
            conf,
        });
    }

    Ok(ListSourcesSinksResp { count, list })
}

pub async fn read_source(device_id: String, source_id: String) -> HaliaResult<ReadSourceSinkResp> {
    let device_type = storage::device::device::read_device_type(&device_id).await?;

    let db_source = storage::device::source_sink::read_one(&source_id).await?;
    let rule_ref_cnt =
        storage::rule::reference::get_rule_ref_info_by_resource_id(&source_id).await?;
    let err = match db_source.status {
        Status::Error => match GLOBAL_DEVICE_MANAGER.get(&device_id) {
            Some(device) => device.read_source_err(&db_source.id).await,
            None => Some("应用未启动！".to_string()),
        },
        _ => None,
    };

    // TODO 优化 db_source的conf克隆的性能问题,
    let conf = get_source_conf_from_db(&device_type, &db_source).await?;

    Ok(ReadSourceSinkResp {
        id: db_source.id,
        name: db_source.name,
        conf,
        status: db_source.status,
        err,
        rule_ref_cnt,
    })
}

pub async fn device_update_source(
    device_id: String,
    source_id: String,
    req: SourceSinkCreateUpdateReq,
) -> HaliaResult<()> {
    let device_conf_type = storage::device::device::read_conf_type(&device_id).await?;
    if device_conf_type == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能修改源。".to_string()));
    }

    let db_source = storage::device::source_sink::read_one(&source_id).await?;
    if req.conf != db_source.conf {
        update_source_conf(device_id, source_id.clone(), req.conf.clone()).await?;
    }

    storage::device::source_sink::update(&source_id, req).await?;
    Ok(())
}

// pub async fn device_template_update_source(
//     device_id: String,
//     source_id: String,
//     conf_type: &ConfType,
//     conf: serde_json::Value,
// ) -> HaliaResult<()> {
//     let mode = match conf_type {
//         ConfType::Customize => update_customize_mode_source(&device_id, &source_id, conf).await?,
//         ConfType::Template => {
//             update_template_mode_source_customize_conf(&device_id, &source_id, conf).await?
//         }
//     };
//     Ok(())
// }

async fn update_source_conf(
    device_id: String,
    source_id: String,
    conf: serde_json::Value,
) -> HaliaResult<()> {
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        device.update_source(&source_id, conf).await?;
    }

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
    if storage::rule::reference::count_cnt_by_resource_id(&source_id, None).await? > 0 {
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

pub async fn create_source_group(
    device_id: String,
    req: DeviceSourceGroupCreateReq,
) -> HaliaResult<()> {
    let device = storage::device::device::read_one(&device_id).await?;
    if device.conf_type != ConfType::Customize {
        return Err(HaliaError::Common("模板设备不能创建源组！".into()));
    }

    debug!("here");

    let source_group = storage::device::source_group::get_by_id(&req.source_group_id).await?;
    if device.device_type != source_group.device_type {
        return Err(HaliaError::Common("源组的设备类型不符合！".into()));
    }
    debug!("here");

    match device.device_type {
        DeviceType::Modbus => {
            let _device_source_group_conf: types::devices::source_group::modbus::DeviceSourceGroupConf = serde_json::from_value(req.conf.clone())?;
            // TODO 判断slave是否符合规范
        }
        _ => return Err(HaliaError::Common("该设备类型不支持增加源组！".into())),
    }

    let source_group_sources =
        storage::device::source_group_source::read_by_source_group_id(&req.source_group_id).await?;

    debug!("here");

    let id = common::get_id();
    storage::device::device_source_group::insert(&id, &device_id, req).await?;

    debug!("here");

    for source_group_source in source_group_sources {
        let soruce_id = common::get_id();
        storage::device::source_sink::source_group_insert_source(
            &soruce_id,
            &device_id,
            &source_group_source.name,
            &source_group_source.id,
        )
        .await?;
    }

    debug!("here");

    Ok(())
}

pub async fn list_source_groups(
    device_id: String,
    pagination: Pagination,
    query: DeviceSourceGroupQueryParams,
) -> HaliaResult<DeviceSourceGroupListResp> {
    let (count, db_device_source_groups) =
        storage::device::device_source_group::search(&device_id, pagination, query).await?;
    let mut list = Vec::with_capacity(db_device_source_groups.len());
    for db_device_source_group in db_device_source_groups {
        list.push(DeviceSourceGroupListItem {
            id: db_device_source_group.id,
            name: db_device_source_group.name,
            source_group_id: db_device_source_group.source_group_id,
            conf: db_device_source_group.conf,
            // TODO
            source_cnt: 0,
        });
    }

    Ok(DeviceSourceGroupListResp { count, list })
}

pub async fn read_source_group(
    device_id: String,
    source_group_id: String,
) -> HaliaResult<DeviceSourceGroupReadResp> {
    let db_device_source_group =
        storage::device::device_source_group::read_one(&source_group_id).await?;

    Ok(DeviceSourceGroupReadResp {
        id: db_device_source_group.id,
        name: db_device_source_group.name,
        source_group_id: db_device_source_group.source_group_id,
        conf: db_device_source_group.conf,
        // TODO
        source_cnt: 0,
    })
}

pub async fn update_source_group(
    device_id: String,
    device_source_group_id: String,
    req: DeviceSourceGroupUpdateReq,
) -> HaliaResult<()> {
    storage::device::device_source_group::update(&device_source_group_id, req).await?;
    Ok(())
}

pub async fn delete_source_group(device_id: String, source_group_id: String) -> HaliaResult<()> {
    storage::device::device_source_group::delete_by_id(&source_group_id).await?;
    Ok(())
}

pub async fn device_create_sink(
    device_id: String,
    req: SourceSinkCreateUpdateReq,
) -> HaliaResult<()> {
    let conf_type = storage::device::device::read_conf_type(&device_id).await?;
    if conf_type == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能创建动作。".to_string()));
    }

    let sink_id = create_sink(&device_id, req.conf.clone()).await?;
    storage::device::source_sink::device_insert_sink(&sink_id, &device_id, req).await?;
    Ok(())
}

pub(crate) async fn device_template_create_sink(
    device_id: &String,
    device_template_sink_id: &String,
    name: String,
    conf: serde_json::Value,
) -> HaliaResult<()> {
    let sink_id = create_sink(&device_id, conf).await?;
    storage::device::source_sink::device_template_insert_sink(
        &sink_id,
        &device_id,
        &name,
        &device_template_sink_id,
    )
    .await?;

    Ok(())
}

async fn create_sink(device_id: &String, conf: serde_json::Value) -> HaliaResult<String> {
    let sink_id = common::get_id();
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(device_id) {
        device.create_sink(sink_id.clone(), conf).await?
    }

    Ok(sink_id)
}

pub async fn list_sinks(
    device_id: String,
    pagination: Pagination,
    query: SourceSinkQueryParams,
) -> HaliaResult<ListSourcesSinksResp> {
    let device_type = storage::device::device::read_device_type(&device_id).await?;
    let (count, db_sinks) =
        storage::device::source_sink::search_sinks(&device_id, pagination, query).await?;
    let mut list = Vec::with_capacity(db_sinks.len());
    for db_sink in db_sinks {
        let rule_ref_cnt =
            storage::rule::reference::get_rule_ref_info_by_resource_id(&db_sink.id).await?;
        let err = match db_sink.status {
            Status::Error => match GLOBAL_DEVICE_MANAGER.get(&device_id) {
                Some(device) => device.read_sink_err(&db_sink.id).await,
                None => Some("应用未启动！".to_string()),
            },
            _ => None,
        };
        let conf = get_sink_conf_from_db(&device_type, &db_sink).await?;

        list.push(ListSourcesSinksItem {
            id: db_sink.id,
            name: db_sink.name,
            status: db_sink.status,
            source_from_type: None,
            err,
            rule_ref_cnt,
            conf,
        });
    }

    Ok(ListSourcesSinksResp { count, list })
}

pub async fn read_sink(device_id: String, sink_id: String) -> HaliaResult<ReadSourceSinkResp> {
    let device_type = storage::device::device::read_device_type(&device_id).await?;

    let db_sink = storage::device::source_sink::read_one(&sink_id).await?;
    let rule_ref_cnt = storage::rule::reference::get_rule_ref_info_by_resource_id(&sink_id).await?;
    let err = match db_sink.status {
        Status::Error => match GLOBAL_DEVICE_MANAGER.get(&device_id) {
            Some(device) => device.read_sink_err(&db_sink.id).await,
            None => Some("应用未启动！".to_string()),
        },
        _ => None,
    };

    // TODO 优化 db_source的conf克隆的性能问题,
    let conf = get_sink_conf_from_db(&device_type, &db_sink).await?;

    Ok(ReadSourceSinkResp {
        id: db_sink.id,
        name: db_sink.name,
        conf,
        status: db_sink.status,
        err,
        rule_ref_cnt,
    })
}

pub async fn device_update_sink(
    device_id: String,
    sink_id: String,
    req: SourceSinkCreateUpdateReq,
) -> HaliaResult<()> {
    let device_conf_type = storage::device::device::read_conf_type(&device_id).await?;
    if device_conf_type == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能修改动作。".to_string()));
    }

    let db_sink = storage::device::source_sink::read_one(&sink_id).await?;
    if req.conf != db_sink.conf {
        update_sink_conf(device_id, sink_id.clone(), req.conf.clone()).await?;
    }

    storage::device::source_sink::update(&sink_id, req).await?;
    Ok(())
}

async fn update_sink_conf(
    device_id: String,
    sink_id: String,
    conf: serde_json::Value,
) -> HaliaResult<()> {
    if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
        device.update_sink(&sink_id, conf).await?;
    }

    Ok(())
}

pub async fn device_delete_sink(device_id: String, sink_id: String) -> HaliaResult<()> {
    if storage::device::device::read_conf_type(&device_id).await? == ConfType::Template {
        return Err(HaliaError::Common("模板设备不能删除源".to_string()));
    }
    delete_sink(device_id, sink_id).await
}

pub(crate) async fn delete_sink(device_id: String, sink_id: String) -> HaliaResult<()> {
    if storage::rule::reference::count_cnt_by_resource_id(&sink_id, None).await? > 0 {
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

#[derive(Clone, Copy)]
enum UpdateConfMode {
    CustomizeMode,
    TemplateModeCustomize,
    TemplateModeTemplate,
}

async fn get_source_conf_from_db(
    device_type: &DeviceType,
    db_source: &storage::device::source_sink::SourceSink,
) -> HaliaResult<serde_json::Value> {
    match db_source.source_from_type {
        types::devices::SourceFromType::Device => Ok(db_source.conf.clone()),
        types::devices::SourceFromType::DeviceTemplate => match &db_source.from_id {
            Some(device_template_source_id) => {
                let db_device_template_source_conf =
                    storage::device::template_source_sink::read_conf(&device_template_source_id)
                        .await?;
                todo!()
                // db_device_template_source.conf
            }
            None => unreachable!(),
        },
        types::devices::SourceFromType::SourceGroup => match &db_source.from_id {
            Some(source_group_source_id) => match device_type {
                DeviceType::Modbus => {
                    let db_source_group_source_conf =
                        storage::device::source_sink::read_conf(&source_group_source_id).await?;
                    let template_conf: types::devices::source_group::modbus::TemplateConf =
                        serde_json::from_value(db_source_group_source_conf)?;
                    let customize_conf: types::devices::source_group::modbus::CustomizeConf =
                        serde_json::from_value(db_source.conf.clone())?;
                    let conf = types::devices::device::modbus::SourceConf {
                        slave: customize_conf.slave,
                        field: template_conf.field,
                        area: template_conf.area,
                        data_type: template_conf.data_type,
                        address: template_conf.address,
                        interval: template_conf.interval,
                        metadatas: customize_conf.metadatas,
                    };
                    let conf = serde_json::to_value(conf)?;
                    Ok(conf)
                }
                _ => unreachable!(),
            },
            None => unreachable!(),
        },
    }
}

async fn get_sink_conf_from_db(
    device_type: &DeviceType,
    db_sink: &storage::device::source_sink::SourceSink,
) -> HaliaResult<serde_json::Value> {
    match db_sink.source_from_type {
        types::devices::SourceFromType::Device => Ok(db_sink.conf.clone()),
        types::devices::SourceFromType::DeviceTemplate => match &db_sink.from_id {
            Some(device_template_source_id) => {
                let db_device_template_source_conf =
                    storage::device::template_source_sink::read_conf(&device_template_source_id)
                        .await?;
                todo!()
                // db_device_template_source.conf
            }
            None => unreachable!(),
        },
        _ => unreachable!(),
    }
}

use common::error::{HaliaError, HaliaResult};
use types::{
    devices::{
        device_template::{self, source_sink, CreateReq, QueryParams, SearchResp, UpdateReq},
        ConfType, DeviceType,
    },
    BaseConf, Pagination,
};

use crate::{modbus, GLOBAL_DEVICE_MANAGER};

pub async fn create_device_template(req: CreateReq) -> HaliaResult<()> {
    match &req.device_type {
        DeviceType::Modbus => modbus::template::validate_device_template_conf(req.conf.clone())?,
        // DeviceType::Opcua => opcua::validate_conf(&req.conf.ext)?,
        DeviceType::Opcua => todo!(),
        // DeviceType::Coap => coap::validate_conf(&req.conf.ext)?,
        DeviceType::Coap => todo!(),
    }

    let id = common::get_id();
    storage::device::template::insert(&id, req).await?;

    Ok(())
}

pub async fn search_device_templates(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<SearchResp> {
    let (count, db_device_templates) =
        storage::device::template::search(pagination, query_params).await?;
    let mut resp_device_templates = vec![];
    for db_device_template in db_device_templates {
        resp_device_templates.push(device_template::SearchItemResp {
            id: db_device_template.id,
            req: CreateReq {
                device_type: db_device_template.device_type.try_into()?,
                base: BaseConf {
                    name: db_device_template.name,
                    desc: db_device_template
                        .des
                        .map(|des| String::from_utf8(des).unwrap()),
                },
                conf: serde_json::from_slice(&db_device_template.conf)?,
            },
        });
    }

    Ok(SearchResp {
        total: count,
        data: resp_device_templates,
    })
}

pub async fn update_device_template(id: String, req: UpdateReq) -> HaliaResult<()> {
    // storage::device::update_conf(&device_id, req).await?;

    Ok(())
}

pub async fn delete_device_template(id: String) -> HaliaResult<()> {
    if storage::device::device::count_by_template_id(&id).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    storage::device::template::delete_by_id(&id).await?;
    Ok(())
}

pub async fn create_source(
    device_template_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    if req.conf_type == ConfType::Template && req.template_id.is_none() {
        return Err(HaliaError::Common("模板ID不能为空".to_string()));
    }

    let device_type: DeviceType = storage::device::template::read_device_type(&device_template_id)
        .await?
        .try_into()?;
    match device_type {
        DeviceType::Modbus => modbus::validate_source_conf(&req.conf)?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
        // DeviceType::Opcua => opcua::validate_source_conf(&req.conf)?,
        // DeviceType::Coap => coap::validate_source_conf(&req.conf)?,
    }

    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;
    let device_source_req = types::devices::device::source_sink::CreateUpdateReq {
        conf_type: req.conf_type.clone(),
        template_id: req.template_id.clone(),
        base: req.base.clone(),
        conf: req.conf.clone(),
    };
    match req.conf_type {
        ConfType::Template => {
            let template_conf = storage::device::source_sink_template::read_conf(
                // 函数入口处即进行了验证，此处永远不会panic
                req.template_id.as_ref().unwrap(),
            )
            .await?;
            let template_conf: serde_json::Value = serde_json::from_slice(&template_conf)?;

            for device_id in device_ids {
                let device_source_id = common::get_id();
                if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
                    device
                        .create_template_source(
                            device_source_id.clone(),
                            req.conf.clone(),
                            template_conf.clone(),
                        )
                        .await?;
                }
                storage::device::source_sink::insert_source(
                    &device_source_id,
                    &device_id,
                    device_source_req.clone(),
                )
                .await?;
            }
        }
        ConfType::Customize => {
            for device_id in device_ids {
                let device_source_id = common::get_id();
                if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
                    device
                        .create_customize_source(device_source_id.clone(), req.conf.clone())
                        .await?
                }
                storage::device::source_sink::insert_source(
                    &device_source_id,
                    &device_id,
                    device_source_req.clone(),
                )
                .await?;
            }
        }
    }

    let source_id = common::get_id();
    storage::device::template_source_sink::insert_source(&source_id, &device_template_id, req)
        .await?;
    Ok(())
}

pub async fn search_sources(
    device_template_id: String,
    pagination: Pagination,
    query: source_sink::QueryParams,
) -> HaliaResult<source_sink::SearchResp> {
    let (count, db_sources) = storage::device::template_source_sink::search_sources(
        &device_template_id,
        pagination,
        query,
    )
    .await?;

    let sources: Vec<_> = db_sources
        .into_iter()
        .map(|x| source_sink::SearchItemResp {
            id: x.id.clone(),
            req: source_sink::CreateUpdateReq {
                conf_type: x.conf_type.try_into().unwrap(),
                template_id: x.template_id,
                base: BaseConf {
                    name: x.name,
                    desc: x.des.map(|desc| String::from_utf8(desc).unwrap()),
                },
                conf: serde_json::from_slice(&x.conf).unwrap(),
            },
        })
        .collect();

    Ok(source_sink::SearchResp {
        total: count,
        data: sources,
    })
}

pub async fn update_source(
    device_tempalte_id: String,
    source_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    todo!()
}

pub async fn delete_source(device_tempalte_id: String, source_id: String) -> HaliaResult<()> {
    todo!()
}

pub async fn create_sink(
    device_template_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    if req.conf_type == ConfType::Template && req.template_id.is_none() {
        return Err(HaliaError::Common("模板ID不能为空".to_string()));
    }

    let device_type: DeviceType = storage::device::template::read_device_type(&device_template_id)
        .await?
        .try_into()?;
    match device_type {
        DeviceType::Modbus => modbus::validate_sink_conf(&req.conf)?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
        // DeviceType::Opcua => opcua::validate_source_conf(&req.conf)?,
        // DeviceType::Coap => coap::validate_source_conf(&req.conf)?,
    }

    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;
    let device_sink_req = types::devices::device::source_sink::CreateUpdateReq {
        conf_type: req.conf_type.clone(),
        template_id: req.template_id.clone(),
        base: req.base.clone(),
        conf: req.conf.clone(),
    };
    match req.conf_type {
        ConfType::Template => {
            let template_conf = storage::device::source_sink_template::read_conf(
                // 函数入口处即进行了验证，此处永远不会panic
                req.template_id.as_ref().unwrap(),
            )
            .await?;
            let template_conf: serde_json::Value = serde_json::from_slice(&template_conf)?;

            for device_id in device_ids {
                let device_sink_id = common::get_id();
                if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
                    device
                        .create_template_sink(
                            device_sink_id.clone(),
                            req.conf.clone(),
                            template_conf.clone(),
                        )
                        .await?;
                }
                storage::device::source_sink::insert_sink(
                    &device_sink_id,
                    &device_id,
                    device_sink_req.clone(),
                )
                .await?;
            }
        }
        ConfType::Customize => {
            for device_id in device_ids {
                let device_sink_id = common::get_id();
                if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
                    device
                        .create_customize_sink(device_sink_id.clone(), req.conf.clone())
                        .await?
                }
                storage::device::source_sink::insert_sink(
                    &device_sink_id,
                    &device_id,
                    device_sink_req.clone(),
                )
                .await?;
            }
        }
    }

    let source_id = common::get_id();
    storage::device::template_source_sink::insert_source(&source_id, &device_template_id, req)
        .await?;
    Ok(())
}

pub async fn search_sinks(
    device_template_id: String,
    pagination: Pagination,
    query: source_sink::QueryParams,
) -> HaliaResult<source_sink::SearchResp> {
    let (count, db_sinks) =
        storage::device::template_source_sink::search_sinks(&device_template_id, pagination, query)
            .await?;

    let sinks: Vec<_> = db_sinks
        .into_iter()
        .map(|x| source_sink::SearchItemResp {
            id: x.id.clone(),
            req: source_sink::CreateUpdateReq {
                conf_type: x.conf_type.try_into().unwrap(),
                template_id: x.template_id,
                base: BaseConf {
                    name: x.name,
                    desc: x.des.map(|desc| String::from_utf8(desc).unwrap()),
                },
                conf: serde_json::from_slice(&x.conf).unwrap(),
            },
        })
        .collect();

    Ok(source_sink::SearchResp {
        total: count,
        data: sinks,
    })
}

pub async fn update_sink(
    device_template_id: String,
    sink_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    todo!()
}

pub async fn delete_sink(device_template_id: String, source_id: String) -> HaliaResult<()> {
    todo!()
}

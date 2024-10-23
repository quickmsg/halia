use common::error::HaliaResult;
use types::{
    devices::{
        device_template::{
            source_sink, CreateReq, QueryParams, SearchItemResp, SearchResp, UpdateReq,
        },
        DeviceType,
    },
    BaseConf, Pagination,
};

use crate::modbus;

pub async fn create(req: CreateReq) -> HaliaResult<()> {
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

pub async fn search(pagination: Pagination, query_params: QueryParams) -> HaliaResult<SearchResp> {
    let (count, db_device_templates) =
        storage::device::template::search(pagination, query_params).await?;
    let mut resp_device_templates = vec![];
    for db_device_template in db_device_templates {
        resp_device_templates.push(SearchItemResp {
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

pub async fn update(id: String, req: UpdateReq) -> HaliaResult<()> {
    // storage::device::update_conf(&device_id, req).await?;

    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    todo!()
}

pub async fn create_source(
    device_template_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    todo!()
}

pub async fn search_sources(
    device_template_id: String,
    pagination: Pagination,
    query: source_sink::QueryParams,
) -> HaliaResult<source_sink::SearchResp> {
    todo!()
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
    todo!()
}

pub async fn search_sinks(
    template_id: String,
    pagination: Pagination,
    query: source_sink::QueryParams,
) -> HaliaResult<source_sink::SearchResp> {
    todo!()
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

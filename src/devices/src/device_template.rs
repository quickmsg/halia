use common::error::HaliaResult;
use types::{
    devices::device_template::{CreateUpdateReq, QueryParams, SearchItemResp, SearchResp},
    BaseConf, Pagination,
};

pub async fn create(id: String, req: CreateUpdateReq) -> HaliaResult<()> {
    // TODO validate
    // match &req.typ {
    //     DeviceType::Modbus => modbus::validate_conf(&req.conf.ext)?,
    //     DeviceType::Opcua => opcua::validate_conf(&req.conf.ext)?,
    //     DeviceType::Coap => coap::validate_conf(&req.conf.ext)?,
    // }

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
            typ: db_device_template.typ.try_into()?,
            base: BaseConf {
                name: db_device_template.name,
                desc: db_device_template
                    .des
                    .map(|desc| String::from_utf8(desc).unwrap()),
            },
            ext: serde_json::from_slice(&db_device_template.conf)?,
        });
    }

    Ok(SearchResp {
        total: count,
        data: resp_device_templates,
    })
}

pub async fn update(id: String, req: CreateUpdateReq) -> HaliaResult<()> {
    // storage::device::update_conf(&device_id, req).await?;

    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    todo!()
}

pub async fn create_source(template_id: String) -> HaliaResult<()> {
    todo!()
}

pub async fn searc_sources(template_id: String) -> HaliaResult<()> {
    todo!()
}

pub async fn update_source(id: String) -> HaliaResult<()> {
    todo!()
}

pub async fn delete_source(id: String) -> HaliaResult<()> {
    todo!()
}

pub async fn create_sink(template_id: String) -> HaliaResult<()> {
    todo!()
}

pub async fn search_sinks(template_id: String) -> HaliaResult<()> {
    todo!()
}

pub async fn delete_sink(id: String) -> HaliaResult<()> {
    todo!()
}
use common::error::{HaliaError, HaliaResult};
use types::{
    devices::{
        source_sink_template::{CreateReq, QueryParams, SearchItemResp, SearchResp, UpdateReq},
        DeviceType,
    },
    BaseConf, Pagination,
};

use crate::{modbus, GLOBAL_DEVICE_MANAGER};

pub async fn create(req: CreateReq) -> HaliaResult<()> {
    match req.device_type {
        DeviceType::Modbus => modbus::source_template::validate(&req.conf)?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    }

    let id = common::get_id();
    storage::device::source_sink_template::insert(&id, storage::SourceSinkType::Source, req)
        .await?;

    Ok(())
}

pub async fn search_source_templates(
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchResp> {
    let (count, db_sources) =
        storage::device::source_sink_template::search_source_templates(pagination, query).await?;

    let mut source_templates = vec![];
    for db_source_template in db_sources {
        source_templates.push(transer_db_source_sink_template_to_resp(db_source_template)?);
    }

    Ok(SearchResp {
        total: count,
        data: source_templates,
    })
}

pub async fn update(id: String, req: UpdateReq) -> HaliaResult<()> {
    let old_conf = storage::device::source_sink_template::read_conf(&id).await?;
    let old_conf: serde_json::Value = serde_json::from_slice(&old_conf)?;
    if old_conf != req.conf {
        let sources = storage::device::source_sink::read_sources_by_template_id(&id).await?;
        for source in sources {
            if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&source.device_id) {
                let customize_conf: serde_json::Value = serde_json::from_slice(&source.conf)?;
                device
                    .update_template_source(&source.id, customize_conf, req.conf.clone())
                    .await?;
            }
        }
    }

    storage::device::source_sink_template::update(&id, req).await?;
    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    if storage::device::source_sink::count_by_template_id(&id).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }
    storage::device::source_sink_template::delete_by_id(&id).await?;
    Ok(())
}

pub async fn create_sink_template(req: CreateReq) -> HaliaResult<()> {
    // match req.device_type {
    //     DeviceType::Modbus => modbus::sink_template::validate(&req.ext)?,
    //     DeviceType::Opcua => todo!(),
    //     DeviceType::Coap => todo!(),
    // }
    todo!()
}

fn transer_db_source_sink_template_to_resp(
    db_template: storage::device::source_sink_template::SourceSinkTemplate,
) -> HaliaResult<SearchItemResp> {
    Ok(SearchItemResp {
        id: db_template.id,
        req: CreateReq {
            device_type: db_template.device_type.try_into()?,
            base: BaseConf {
                name: db_template.name,
                desc: db_template.des.map(|desc| String::from_utf8(desc).unwrap()),
            },
            conf: serde_json::from_slice(&db_template.conf)?,
        },
    })
}

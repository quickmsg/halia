use common::error::{HaliaError, HaliaResult};
use types::{
    devices::{
        source_sink_template::{CreateReq, QueryParams, SearchItemResp, SearchResp, UpdateReq},
        DeviceType,
    },
    Pagination,
};

use crate::{modbus, GLOBAL_DEVICE_MANAGER};

pub async fn create_source_template(req: CreateReq) -> HaliaResult<()> {
    match req.device_type {
        DeviceType::Modbus => modbus::template::validate_source_template_conf(req.conf.clone())?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    }

    let id = common::get_id();
    storage::device::source_sink_template::insert_source(&id, req).await?;

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
        source_templates.push(SearchItemResp {
            id: db_source_template.id,
            req: CreateReq {
                name: db_source_template.name,
                device_type: db_source_template.device_type,
                conf: db_source_template.conf,
            },
        });
    }

    Ok(SearchResp {
        total: count,
        data: source_templates,
    })
}

pub async fn update_source_template(id: String, req: UpdateReq) -> HaliaResult<()> {
    let old_conf = storage::device::source_sink_template::read_conf(&id).await?;
    if old_conf != req.conf {
        let sources = storage::device::source_sink::read_sources_by_template_id(&id).await?;
        for source in sources {
            if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&source.device_id) {
                device
                    .update_template_source(&source.id, source.conf, req.conf.clone())
                    .await?;
            }
        }
    }

    storage::device::source_sink_template::update(&id, req).await?;
    Ok(())
}

pub async fn delete_source_template(id: String) -> HaliaResult<()> {
    if storage::device::source_sink::count_by_template_id(&id).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }
    storage::device::source_sink_template::delete_by_id(&id).await?;
    Ok(())
}

pub async fn create_sink_template(req: CreateReq) -> HaliaResult<()> {
    match req.device_type {
        DeviceType::Modbus => modbus::template::validate_sink_template_conf(req.conf.clone())?,
        DeviceType::Opcua => todo!(),
        DeviceType::Coap => todo!(),
    }

    let id = common::get_id();
    storage::device::source_sink_template::insert_sink(&id, req).await?;
    Ok(())
}

pub async fn search_sink_templates(
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchResp> {
    let (count, db_sinks) =
        storage::device::source_sink_template::search_sink_templates(pagination, query).await?;

    let mut sink_templates = vec![];
    for db_sink_template in db_sinks {
        sink_templates.push(SearchItemResp {
            id: db_sink_template.id,
            req: CreateReq {
                name: db_sink_template.name,
                device_type: db_sink_template.device_type,
                conf: db_sink_template.conf,
            },
        });
    }

    Ok(SearchResp {
        total: count,
        data: sink_templates,
    })
}

pub async fn update_sink_template(id: String, req: UpdateReq) -> HaliaResult<()> {
    let old_conf = storage::device::source_sink_template::read_conf(&id).await?;
    if old_conf != req.conf {
        let sinks = storage::device::source_sink::read_sinks_by_template_id(&id).await?;
        for sink in sinks {
            if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&sink.device_id) {
                device
                    .update_template_sink(&sink.id, sink.conf, req.conf.clone())
                    .await?;
            }
        }
    }

    storage::device::source_sink_template::update(&id, req).await?;
    Ok(())
}

pub async fn delete_sink_template(id: String) -> HaliaResult<()> {
    if storage::device::source_sink::count_by_template_id(&id).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }
    storage::device::source_sink_template::delete_by_id(&id).await?;
    Ok(())
}

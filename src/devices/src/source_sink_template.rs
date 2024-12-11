use common::error::{HaliaError, HaliaResult};
use types::{
    devices::{
        source_sink_template::{
            CreateReq, ListItem, ListResp, ListSinkReferencesItem, ListSinkReferencesResp,
            ListSourceReferencesItem, ListSourceReferencesResp, QueryParams, ReadResp, UpdateReq,
        },
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

pub async fn list_source_templates(
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<ListResp> {
    let (count, db_sources) =
        storage::device::source_sink_template::search_source_templates(pagination, query).await?;
    let mut list = vec![];
    for db_source_template in db_sources {
        // TODO 再次统计模板中的引用数量
        let reference_cnt =
            storage::device::source_sink::count_by_template_id(&db_source_template.id).await?;
        list.push(ListItem {
            id: db_source_template.id,
            name: db_source_template.name,
            device_type: db_source_template.device_type,
            reference_cnt,
        });
    }

    Ok(ListResp { count, list })
}

pub async fn read_source_template(id: String) -> HaliaResult<ReadResp> {
    let db_source_template = storage::device::source_sink_template::read_one(&id).await?;
    let reference_cnt = storage::device::source_sink::count_by_template_id(&id).await?;
    Ok(ReadResp {
        id: db_source_template.id,
        name: db_source_template.name,
        device_type: db_source_template.device_type,
        reference_cnt,
        conf: db_source_template.conf,
    })
}

pub async fn update_source_template(id: String, req: UpdateReq) -> HaliaResult<()> {
    let old_conf = storage::device::source_sink_template::read_conf(&id).await?;
    if old_conf != req.conf {
        let device_sources = storage::device::source_sink::read_sources_by_template_id(&id).await?;
        for device_source in device_sources {
            super::update_template_mode_source_template_conf(
                &device_source.device_id,
                &device_source.id,
                req.conf.clone(),
            )
            .await?;
        }

        let device_template_sources =
            storage::device::template_source_sink::read_sources_by_template_id(&id).await?;
        for device_template_source in device_template_sources {
            todo!()
        }

        // todo 更新设备模板中的源
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

pub async fn list_source_template_references(
    template_id: String,
    pagination: Pagination,
) -> HaliaResult<ListSourceReferencesResp> {
    let (count, db_sources) =
        storage::device::source_sink::search_by_template_id(&template_id, pagination).await?;
    let mut list = vec![];
    for db_source in db_sources {
        let device = storage::device::device::read_one(&db_source.device_id).await?;
        list.push(ListSourceReferencesItem {
            device_id: device.id,
            device_name: device.name,
            source_id: db_source.id,
            source_name: db_source.name,
        });
    }

    Ok(ListSourceReferencesResp { count, list })
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

pub async fn list_sink_templates(
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<ListResp> {
    let (count, db_sinks) =
        storage::device::source_sink_template::search_sink_templates(pagination, query).await?;
    let mut list = vec![];
    for db_sink_template in db_sinks {
        let reference_cnt =
            storage::device::source_sink::count_by_template_id(&db_sink_template.id).await?;
        list.push(ListItem {
            id: db_sink_template.id,
            name: db_sink_template.name,
            device_type: db_sink_template.device_type,
            reference_cnt,
        });
    }

    Ok(ListResp { count, list })
}

pub async fn read_sink_template(id: String) -> HaliaResult<ReadResp> {
    let db_source_template = storage::device::source_sink_template::read_one(&id).await?;
    let reference_cnt = storage::device::source_sink::count_by_template_id(&id).await?;
    Ok(ReadResp {
        id: db_source_template.id,
        name: db_source_template.name,
        device_type: db_source_template.device_type,
        reference_cnt,
        conf: db_source_template.conf,
    })
}

pub async fn update_sink_template(id: String, req: UpdateReq) -> HaliaResult<()> {
    let old_conf = storage::device::source_sink_template::read_conf(&id).await?;
    if old_conf != req.conf {
        let sinks = storage::device::source_sink::read_sinks_by_template_id(&id).await?;
        for sink in sinks {
            if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&sink.device_id) {
                device
                    .update_template_sink(&sink.id, sink.conf.unwrap(), req.conf.clone())
                    .await?;
            }
        }

        // TODO
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

pub async fn list_sink_template_references(
    template_id: String,
    pagination: Pagination,
) -> HaliaResult<ListSinkReferencesResp> {
    let (count, db_sinks) =
        storage::device::source_sink::search_by_template_id(&template_id, pagination).await?;
    let mut list = vec![];
    for db_sink in db_sinks {
        let device = storage::device::device::read_one(&db_sink.device_id).await?;
        list.push(ListSinkReferencesItem {
            device_id: device.id,
            device_name: device.name,
            sink_id: db_sink.id,
            sink_name: db_sink.name,
        });
    }

    Ok(ListSinkReferencesResp { count, list })
}

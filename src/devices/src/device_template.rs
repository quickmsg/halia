use common::error::{HaliaError, HaliaResult};
use types::{
    devices::{
        device_template::{self, source_sink, CreateReq, ListResp, QueryParams, ReadResp},
        DeviceType,
    },
    Pagination,
};

use crate::{modbus, opcua, GLOBAL_DEVICE_MANAGER};

pub async fn create_device_template(req: CreateReq) -> HaliaResult<()> {
    match &req.device_type {
        DeviceType::Modbus => modbus::template::validate_device_template_conf(req.conf.clone())?,
        DeviceType::Opcua => opcua::template::validate_device_template_conf(req.conf.clone())?,
        // DeviceType::Coap => coap::validate_conf(&req.conf.ext)?,
        DeviceType::Coap => todo!(),
    }

    let id = common::get_id();
    storage::device::template::insert(&id, req).await?;

    Ok(())
}

pub async fn list_device_templates(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<ListResp> {
    let (count, db_device_templates) =
        storage::device::template::search(pagination, query_params).await?;
    let mut list = vec![];
    for db_device_template in db_device_templates {
        let reference_cnt =
            storage::device::device::count_by_template_id(&db_device_template.id).await?;
        list.push(device_template::ListItem {
            id: db_device_template.id,
            name: db_device_template.name,
            device_type: db_device_template.device_type,
            reference_cnt,
            can_delete: reference_cnt == 0,
        });
    }

    Ok(ListResp { count, list })
}

pub async fn read_device_template(id: String) -> HaliaResult<ReadResp> {
    let db_device_template = storage::device::template::read_one(&id).await?;
    let reference_cnt = storage::device::device::count_by_template_id(&id).await?;
    Ok(ReadResp {
        id: db_device_template.id,
        name: db_device_template.name,
        device_type: db_device_template.device_type,
        reference_cnt,
        can_delete: reference_cnt == 0,
        conf: db_device_template.conf,
    })
}

pub async fn update_device_template(
    id: String,
    req: device_template::UpdateReq,
) -> HaliaResult<()> {
    let device_ids = storage::device::device::read_ids_by_template_id(&id).await?;
    for device_id in device_ids {
        let customize_conf = storage::device::device::read_conf(&device_id).await?;
        if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
            device
                .update_template_conf(customize_conf, req.conf.clone())
                .await?;
        }
    }

    storage::device::template::update(&id, req).await?;
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
    let device_source_req = types::devices::CreateUpdateSourceSinkReq {
        name: req.name.clone(),
        conf_type: req.conf_type.clone(),
        template_id: req.template_id.clone(),
        conf: req.conf.clone(),
    };
    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;

    let source_id = common::get_id();

    for device_id in device_ids {
        super::create_source(device_id, Some(&source_id), device_source_req.clone()).await?;
    }

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
                name: x.name,
                conf_type: x.conf_type,
                template_id: x.template_id,
                conf: x.conf,
            },
        })
        .collect();

    Ok(source_sink::SearchResp {
        total: count,
        data: sources,
    })
}

pub async fn update_source(
    device_template_id: String,
    source_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    let device_source_req = types::devices::CreateUpdateSourceSinkReq {
        name: req.name.clone(),
        conf_type: req.conf_type.clone(),
        template_id: req.template_id.clone(),
        conf: req.conf.clone(),
    };
    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;
    for device_id in device_ids {
        let device_source_id =
            storage::device::source_sink::read_id_by_device_template_source_sink_id(
                &device_id, &source_id,
            )
            .await?;
        super::update_source(device_id, device_source_id, device_source_req.clone()).await?;
    }

    storage::device::template_source_sink::update(&source_id, req).await?;
    Ok(())
}

pub async fn delete_source(device_template_id: String, source_id: String) -> HaliaResult<()> {
    let ids =
        storage::device::source_sink::read_many_ids_by_device_template_source_sink_id(&source_id)
            .await?;
    if storage::rule::reference::count_cnt_by_many_resource_ids(&ids).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;
    for device_id in device_ids {
        let device_source_id =
            storage::device::source_sink::read_id_by_device_template_source_sink_id(
                &device_id, &source_id,
            )
            .await?;
        super::delete_source(device_id, device_source_id).await?;
    }

    storage::device::template_source_sink::delete_by_id(&source_id).await?;
    Ok(())
}

pub async fn create_sink(
    device_template_id: String,
    req: source_sink::CreateUpdateReq,
) -> HaliaResult<()> {
    let device_sink_req = types::devices::CreateUpdateSourceSinkReq {
        name: req.name.clone(),
        conf_type: req.conf_type.clone(),
        template_id: req.template_id.clone(),
        conf: req.conf.clone(),
    };
    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;

    let sink_id = common::get_id();
    for device_id in device_ids {
        super::create_sink(device_id, Some(&sink_id), device_sink_req.clone()).await?;
    }

    storage::device::template_source_sink::insert_sink(&sink_id, &device_template_id, req).await?;
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
            id: x.id,
            req: source_sink::CreateUpdateReq {
                name: x.name,
                conf_type: x.conf_type,
                template_id: x.template_id,
                conf: x.conf,
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
    let device_sink_req = types::devices::CreateUpdateSourceSinkReq {
        name: req.name.clone(),
        conf_type: req.conf_type.clone(),
        template_id: req.template_id.clone(),
        conf: req.conf.clone(),
    };
    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;
    for device_id in device_ids {
        let device_source_id =
            storage::device::source_sink::read_id_by_device_template_source_sink_id(
                &device_id, &sink_id,
            )
            .await?;
        super::update_source(device_id, device_source_id, device_sink_req.clone()).await?;
    }

    storage::device::template_source_sink::update(&sink_id, req).await?;
    Ok(())
}

pub async fn delete_sink(device_template_id: String, sink_id: String) -> HaliaResult<()> {
    let ids =
        storage::device::source_sink::read_many_ids_by_device_template_source_sink_id(&sink_id)
            .await?;
    if ids.len() > 0 {
        if storage::rule::reference::count_cnt_by_many_resource_ids(&ids).await? > 0 {
            return Err(HaliaError::DeleteRefing);
        }
    }

    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;
    for device_id in device_ids {
        let device_sink_id =
            storage::device::source_sink::read_id_by_device_template_source_sink_id(
                &device_id, &sink_id,
            )
            .await?;
        super::delete_sink(device_id, device_sink_id).await?;
    }

    storage::device::template_source_sink::delete_by_id(&sink_id).await?;
    Ok(())
}

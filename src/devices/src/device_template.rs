use common::error::{HaliaError, HaliaResult};
use types::{
    devices::{
        device_template::{
            self, source_sink, CreateReq, ListResp, QueryParams, ReadResp, UpdateReq,
        },
        DeviceType, SourceSinkCreateUpdateReq,
    },
    Pagination,
};

use crate::{
    device_template_create_sink, device_template_create_source, modbus, opcua,
    GLOBAL_DEVICE_MANAGER,
};

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
        let source_cnt =
            storage::device::template_source_sink::count_sources_by_device_template_id(
                &db_device_template.id,
            )
            .await?;
        let sink_cnt = storage::device::template_source_sink::count_sinks_by_device_template_id(
            &db_device_template.id,
        )
        .await?;
        let reference_cnt =
            storage::device::device::count_by_template_id(&db_device_template.id).await?;
        list.push(device_template::ListItem {
            id: db_device_template.id,
            name: db_device_template.name,
            device_type: db_device_template.device_type,
            reference_cnt,
            source_cnt,
            sink_cnt,
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
        conf: db_device_template.conf,
    })
}

pub async fn update_device_template(id: String, req: UpdateReq) -> HaliaResult<()> {
    let db_conf = storage::device::template::read_conf(&id).await?;
    if req.conf != db_conf {
        let device_ids = storage::device::device::read_ids_by_template_id(&id).await?;
        device_ids.into_iter().for_each(|device_id| {
            let conf = req.conf.clone();
            tokio::spawn(async move {
                if let Some(mut device) = GLOBAL_DEVICE_MANAGER.get_mut(&device_id) {
                    let _ = device
                        .update_conf(crate::UpdateConfMode::TemplateModeTemplate, conf)
                        .await;
                }
            });
        });
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
    req: SourceSinkCreateUpdateReq,
) -> HaliaResult<()> {
    // let device_type: DeviceType =
    //     storage::device::template::read_device_type(&device_template_id).await?;
    // match device_type {
    //     DeviceType::Modbus => modbus::validate_source_conf(&req.conf)?,
    //     DeviceType::Opcua => opcua::validate_source_conf(&req.conf)?,
    //     DeviceType::Coap => coap::validate_source_conf(&req.conf)?,
    // }

    let name = req.name.clone();
    let conf = req.conf.clone();

    let device_template_source_id = common::get_id();
    storage::device::template_source_sink::insert_source(
        &device_template_source_id,
        &device_template_id,
        req,
    )
    .await?;

    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;
    for device_id in device_ids {
        device_template_create_source(
            device_id,
            &device_template_source_id,
            name.clone(),
            conf.clone(),
        )
        .await?;
    }

    Ok(())
}

pub async fn list_sources(
    device_template_id: String,
    pagination: Pagination,
    query: source_sink::QueryParams,
) -> HaliaResult<source_sink::ListResp> {
    let (count, db_sources) = storage::device::template_source_sink::search_sources(
        &device_template_id,
        pagination,
        query,
    )
    .await?;

    let list: Vec<_> = db_sources
        .into_iter()
        .map(|x| source_sink::ListItem {
            id: x.id.clone(),
            name: x.name,
        })
        .collect();

    Ok(source_sink::ListResp { count, list })
}

pub async fn read_source(
    _device_template_id: String,
    source_id: String,
) -> HaliaResult<source_sink::ReadResp> {
    let db_source = storage::device::template_source_sink::read_one(&source_id).await?;
    Ok(source_sink::ReadResp {
        id: db_source.id,
        name: db_source.name,
        conf: db_source.conf,
    })
}

pub async fn update_source(
    device_template_id: String,
    source_id: String,
    req: SourceSinkCreateUpdateReq,
) -> HaliaResult<()> {
    let db_source = storage::device::template_source_sink::read_one(&source_id).await?;
    if req.conf != db_source.conf {
        let device_ids =
            storage::device::device::read_ids_by_template_id(&device_template_id).await?;
        for device_id in device_ids {
            let device_source_id =
                storage::device::source_sink::read_id_by_device_template_source_sink_id(
                    &device_id, &source_id,
                )
                .await?;
            super::update_source_conf(device_id, device_source_id, req.conf.clone()).await?;
        }
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
    req: SourceSinkCreateUpdateReq,
) -> HaliaResult<()> {
    // let device_type: DeviceType =
    //     storage::device::template::read_device_type(&device_template_id).await?;
    // match device_type {
    //     DeviceType::Modbus => modbus::validate_source_conf(&req.conf)?,
    //     DeviceType::Opcua => opcua::validate_source_conf(&req.conf)?,
    //     DeviceType::Coap => coap::validate_source_conf(&req.conf)?,
    // }

    let name = req.name.clone();
    let conf = req.conf.clone();
    let device_template_sink_id = common::get_id();
    storage::device::template_source_sink::insert_sink(
        &device_template_sink_id,
        &device_template_id,
        req.clone(),
    )
    .await?;

    let device_ids = storage::device::device::read_ids_by_template_id(&device_template_id).await?;
    for device_id in device_ids {
        device_template_create_sink(
            &device_id,
            &device_template_sink_id,
            name.clone(),
            conf.clone(),
        )
        .await?;
    }

    Ok(())
}

pub async fn list_sinks(
    device_template_id: String,
    pagination: Pagination,
    query: source_sink::QueryParams,
) -> HaliaResult<source_sink::ListResp> {
    let (count, db_sinks) =
        storage::device::template_source_sink::search_sinks(&device_template_id, pagination, query)
            .await?;

    let list: Vec<_> = db_sinks
        .into_iter()
        .map(|x| source_sink::ListItem {
            id: x.id,
            name: x.name,
        })
        .collect();

    Ok(source_sink::ListResp { count, list })
}

pub async fn read_sink(
    _device_template_id: String,
    sink_id: String,
) -> HaliaResult<source_sink::ReadResp> {
    let db_sink = storage::device::template_source_sink::read_one(&sink_id).await?;
    Ok(source_sink::ReadResp {
        id: db_sink.id,
        name: db_sink.name,
        conf: db_sink.conf,
    })
}

pub async fn update_sink(
    device_template_id: String,
    sink_id: String,
    req: SourceSinkCreateUpdateReq,
) -> HaliaResult<()> {
    let db_sink = storage::device::template_source_sink::read_one(&sink_id).await?;
    if req.conf != db_sink.conf {
        let device_ids =
            storage::device::device::read_ids_by_template_id(&device_template_id).await?;
        for device_id in device_ids {
            let device_sink_id =
                storage::device::source_sink::read_id_by_device_template_source_sink_id(
                    &device_id, &sink_id,
                )
                .await?;
            super::update_sink_conf(device_id, device_sink_id, req.conf.clone()).await?;
        }
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

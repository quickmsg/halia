use common::error::{HaliaError, HaliaResult};
use types::{
    devices::source_group::{
        CreateReq, CreateUpdateSourceReq, ListItem, ListResp, ListSourcesItem, ListSourcesResp,
        QueryParams, ReadResp, ReadSourceResp, SourceQueryParams, UpdateReq,
    },
    Pagination,
};

use crate::source_group_create_source;

pub async fn create_source_group(req: CreateReq) -> HaliaResult<()> {
    match req.device_type {
        types::devices::DeviceType::Modbus => {}
        _ => return Err(HaliaError::NotSupportResource),
    }
    let id = common::get_id();
    storage::device::source_group::insert(&id, req).await?;
    Ok(())
}

pub async fn list_source_groups(
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<ListResp> {
    let (count, db_source_gropus) =
        storage::device::source_group::search(pagination, query).await?;

    let mut list = Vec::with_capacity(db_source_gropus.len());
    for db_source_group in db_source_gropus {
        let source_cnt =
            storage::device::source_group_source::count_by_source_group_id(&db_source_group.id)
                .await?;
        list.push(ListItem {
            id: db_source_group.id,
            name: db_source_group.name,
            device_type: db_source_group.device_type,
            // TODO
            reference_cnt: 0,
            source_cnt,
        });
    }

    Ok(ListResp { count, list })
}

pub async fn read_source_group(id: String) -> HaliaResult<ReadResp> {
    let db_source_group = storage::device::source_group::get_by_id(&id).await?;
    let source_cnt =
        storage::device::source_group_source::count_by_source_group_id(&db_source_group.id).await?;
    Ok(ReadResp {
        id,
        name: db_source_group.name,
        device_type: db_source_group.device_type,
        reference_cnt: 0,
        source_cnt,
    })
}

pub async fn update_source_group(id: String, req: UpdateReq) -> HaliaResult<()> {
    storage::device::source_group::update(&id, req).await?;
    Ok(())
}

pub async fn delete_source_group(id: String) -> HaliaResult<()> {
    // TODO 校验是否可以删除
    storage::device::source_group::delete_by_id(&id).await?;
    Ok(())
}

pub async fn create_source(source_group_id: String, req: CreateUpdateSourceReq) -> HaliaResult<()> {
    let name = req.name.clone();
    let conf = req.conf.clone();

    let source_id = common::get_id();
    storage::device::source_group_source::insert(&source_id, &source_group_id, req).await?;

    let device_ids =
        storage::device::device_source_group::read_device_ids_by_source_group_id(&source_group_id)
            .await?;
    for device_id in device_ids {
        source_group_create_source(&device_id, &source_id, name.clone(), conf.clone()).await?;
    }

    Ok(())
}

pub async fn list_sources(
    source_group_id: String,
    pagination: Pagination,
    query: SourceQueryParams,
) -> HaliaResult<ListSourcesResp> {
    let (count, db_sources) =
        storage::device::source_group_source::search(&source_group_id, pagination, query).await?;
    let mut list = vec![];
    for db_source in db_sources {
        list.push(ListSourcesItem {
            id: db_source.id,
            name: db_source.name,
            conf: db_source.conf,
        });
    }
    Ok(ListSourcesResp { count, list })
}

pub async fn read_source(
    source_group_id: String,
    source_id: String,
) -> HaliaResult<ReadSourceResp> {
    let db_source = storage::device::source_group_source::get_by_id(&source_id).await?;

    Ok(ReadSourceResp {
        id: db_source.id,
        name: db_source.name,
        conf: db_source.conf,
    })
}

pub async fn update_source(
    source_group_id: String,
    source_id: String,
    req: CreateUpdateSourceReq,
) -> HaliaResult<()> {
    storage::device::source_group_source::update(&source_id, req).await?;
    Ok(())
}

pub async fn delete_source(source_group_id: String, source_id: String) -> HaliaResult<()> {
    storage::device::source_group_source::delete_by_id(&source_id).await?;
    Ok(())
}

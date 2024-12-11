use common::error::HaliaResult;
use types::{
    devices::source_group::{
        CreateReq, CreateUpdateSourceReq, ListItem, ListResp, QueryParams, SourceQueryParams,
        UpdateReq,
    },
    Pagination,
};

pub async fn create_source_group(req: CreateReq) -> HaliaResult<()> {
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
        list.push(ListItem {
            id: db_source_group.id,
            name: db_source_group.name,
            device_type: db_source_group.device_type,
            // TODO
            reference_cnt: 0,
        });
    }

    Ok(ListResp { count, list })
}

pub async fn read_source_group(id: String) -> HaliaResult<()> {
    todo!()
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
    let source_id = common::get_id();
    storage::device::source_group_source::insert(&source_id, &source_group_id, req).await?;
    Ok(())
}

pub async fn list_sources(
    source_group_id: String,
    pagination: Pagination,
    query: SourceQueryParams,
) -> HaliaResult<()> {
    let (count, db_sources) =
        storage::device::source_group_source::search(&source_group_id, pagination, query).await?;
    Ok(())
}

pub async fn read_source() {}

pub async fn update_source() {}

pub async fn delete_source() {}

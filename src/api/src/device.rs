use axum::{
    body::Bytes,
    extract::{Path, Query},
    Json,
};
use common::error::HaliaError;
use device::GLOBAL_DEVICE_MANAGER;
use types::{
    device::{
        device::{SearchDeviceResp, SearchSinksResp},
        group::SearchGroupResp,
        point::{CreatePointReq, SearchPointResp, WritePointValueReq},
    },
    SearchResp,
};
use uuid::Uuid;

use crate::{AppResp, DeleteIdsQuery, Pagination};

pub(crate) async fn create_device(req: Bytes) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.create_device(&req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_device(pagination: Query<Pagination>) -> AppResp<SearchDeviceResp> {
    AppResp::with_data(
        GLOBAL_DEVICE_MANAGER
            .search_devices(pagination.p, pagination.s)
            .await,
    )
}

pub(crate) async fn start_device(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.start_device(device_id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn stop_device(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.stop_device(device_id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_device(Path(device_id): Path<Uuid>, body: Bytes) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.update_device(device_id, &body).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_device(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.delete_device(device_id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn create_group(Path(device_id): Path<Uuid>, req: Bytes) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.create_group(device_id, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_group(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchGroupResp> {
    match GLOBAL_DEVICE_MANAGER
        .search_groups(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(groups) => AppResp::with_data(groups),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_group(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    req: Bytes,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .update_group(device_id, group_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_group(Path((device_id, group_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .delete_group(device_id, group_id)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn create_point(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    req: Bytes,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .create_point(device_id, group_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_point(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    pagination: Query<Pagination>,
) -> AppResp<SearchPointResp> {
    match GLOBAL_DEVICE_MANAGER
        .search_point(device_id, group_id, pagination.p, pagination.s)
        .await
    {
        Ok(values) => AppResp::with_data(values),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_point(
    Path((device_id, group_id, point_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(req): Json<CreatePointReq>,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .update_point(device_id, group_id, point_id, &req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn write_point(
    Path((device_id, group_id, point_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(req): Json<WritePointValueReq>,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .write_point_value(device_id, group_id, point_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_points(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Query(query): Query<DeleteIdsQuery>,
) -> AppResp<()> {
    let point_ids: Vec<Uuid> = match query
        .ids
        .split(',')
        .map(|s| s.parse::<Uuid>().map_err(|_e| return HaliaError::ParseErr))
        .collect::<Result<Vec<Uuid>, _>>()
    {
        Ok(ids) => ids,
        Err(e) => return e.into(),
    };

    match GLOBAL_DEVICE_MANAGER
        .delete_points(device_id, group_id, point_ids)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn create_sink(Path(device_id): Path<Uuid>, req: Bytes) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.create_sink(device_id, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn search_sinks(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSinksResp> {
    match GLOBAL_DEVICE_MANAGER
        .search_sinks(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub async fn update_sink(
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
    req: Bytes,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .update_sink(device_id, sink_id, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn delete_sink(Path((device_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.delete_sink(device_id, sink_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn add_source(Path(device_id): Path<Uuid>, req: Bytes) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.add_subscription(device_id, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn add_path(Path(device_id): Path<Uuid>, req: Bytes) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.add_path(device_id, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_paths(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchResp> {
    match GLOBAL_DEVICE_MANAGER
        .search_paths(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_path(
    Path((device_id, path_id)): Path<(Uuid, Uuid)>,
    req: Bytes,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .update_path(device_id, path_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

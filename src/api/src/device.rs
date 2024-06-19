use axum::{
    extract::{Path, Query},
    Json,
};
use common::error::HaliaError;
use device::GLOBAL_DEVICE_MANAGER;
use tracing::{debug, trace};
use types::device::{
    device::{CreateDeviceReq, DeviceDetailResp, SearchDeviceResp, UpdateDeviceReq},
    group::{CreateGroupReq, SearchGroupResp, UpdateGroupReq},
    point::{CreatePointReq, SearchPointResp, WritePointValueReq},
};
use uuid::Uuid;

use crate::{AppResp, DeleteIdsQuery, Pagination};

pub(crate) async fn create_device(Json(req): Json<CreateDeviceReq>) -> AppResp<()> {
    trace!("create_device:{:?}", req);
    match GLOBAL_DEVICE_MANAGER.create_device(None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn read_device(Path(device_id): Path<Uuid>) -> AppResp<DeviceDetailResp> {
    match GLOBAL_DEVICE_MANAGER.read_device(device_id).await {
        Ok(resp) => AppResp::with_data(resp),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_device(pagination: Query<Pagination>) -> AppResp<SearchDeviceResp> {
    AppResp::with_data(
        GLOBAL_DEVICE_MANAGER
            .search_device(pagination.p, pagination.s)
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

pub(crate) async fn update_device(
    Path(device_id): Path<Uuid>,
    Json(req): Json<UpdateDeviceReq>,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER.update_device(device_id, req).await {
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

pub(crate) async fn create_group(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateGroupReq>,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .create_group(device_id, None, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_group(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchGroupResp> {
    match GLOBAL_DEVICE_MANAGER
        .search_group(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(groups) => AppResp::with_data(groups),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_group(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<UpdateGroupReq>,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .update_group(device_id, group_id, &req)
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

pub(crate) async fn create_points(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<Vec<CreatePointReq>>,
) -> AppResp<()> {
    match GLOBAL_DEVICE_MANAGER
        .create_points(
            device_id,
            group_id,
            req.into_iter()
                .map(|req| (None, req))
                .collect::<Vec<(Option<Uuid>, CreatePointReq)>>(),
        )
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
    debug!("update_point:{:?}", req);
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
    debug!("update_point:{:?}", req);
    match GLOBAL_DEVICE_MANAGER
        .write_point_value(device_id, group_id, point_id, &req)
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

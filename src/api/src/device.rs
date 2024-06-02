use axum::{
    extract::{Path, Query},
    Json,
};
use device::GLOBAL_DEVICE_MANAGER;
use serde_json::Value;
use tracing::{debug, trace};
use types::device::{
    CreateDeviceReq, CreateGroupReq, CreatePointReq, DeviceDetailResp, ListDevicesResp,
    ListGroupsResp, ListPointResp,
};

use crate::{AppError, DeleteIdsQuery};

pub(crate) async fn create_device(Json(req): Json<CreateDeviceReq>) -> Result<(), AppError> {
    trace!("create_device:{:?}", req);
    match GLOBAL_DEVICE_MANAGER.create_device(None, req).await {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn read_device(Path(id): Path<u64>) -> Result<Json<DeviceDetailResp>, AppError> {
    trace!("get_device:{:?}", id);
    match GLOBAL_DEVICE_MANAGER.read_device(id).await {
        Ok(device_detail) => Ok(Json(device_detail)),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn read_devices() -> Json<Vec<ListDevicesResp>> {
    Json(GLOBAL_DEVICE_MANAGER.read_devices().await)
}

pub(crate) async fn start_device(Path(id): Path<u64>) -> Result<(), AppError> {
    match GLOBAL_DEVICE_MANAGER.start_device(id).await {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn stop_device(Path(id): Path<u64>) -> Result<(), AppError> {
    match GLOBAL_DEVICE_MANAGER.stop_device(id).await {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn update_device(
    Path(id): Path<u64>,
    Json(conf): Json<Value>,
) -> Result<(), AppError> {
    match GLOBAL_DEVICE_MANAGER.update_device(id, conf).await {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn delete_device(Path(id): Path<u64>) -> Result<(), AppError> {
    match GLOBAL_DEVICE_MANAGER.delete_device(id).await {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn create_group(
    Path(id): Path<u64>,
    Json(create_group): Json<CreateGroupReq>,
) -> Result<(), AppError> {
    match GLOBAL_DEVICE_MANAGER
        .create_group(id, None, create_group)
        .await
    {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn read_groups(
    Path(id): Path<u64>,
) -> Result<Json<Vec<ListGroupsResp>>, AppError> {
    match GLOBAL_DEVICE_MANAGER.read_groups(id).await {
        Ok(groups) => Ok(Json(groups)),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn update_group(
    Path((device_id, group_id)): Path<(u64, u64)>,
    Json(update_group): Json<Value>,
) -> Result<(), AppError> {
    match GLOBAL_DEVICE_MANAGER
        .update_group(device_id, group_id, update_group)
        .await
    {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn delete_groups(
    Path(id): Path<u64>,
    Query(query): Query<DeleteIdsQuery>,
) -> Result<(), AppError> {
    let group_ids: Vec<u64> = query
        .ids
        .split(',')
        .map(|s| match s.parse::<u64>() {
            Ok(id) => id,
            Err(_) => todo!(),
        })
        .collect();
    match GLOBAL_DEVICE_MANAGER.delete_groups(id, group_ids).await {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn create_points(
    Path((device_id, group_id)): Path<(u64, u64)>,
    Json(req): Json<Vec<CreatePointReq>>,
) -> Result<(), AppError> {
    match GLOBAL_DEVICE_MANAGER
        .create_points(
            device_id,
            group_id,
            req.into_iter()
                .map(|req| (None, req))
                .collect::<Vec<(Option<u64>, CreatePointReq)>>(),
        )
        .await
    {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn read_points(
    Path((device_id, group_id)): Path<(u64, u64)>,
) -> Result<Json<Vec<ListPointResp>>, AppError> {
    match GLOBAL_DEVICE_MANAGER.read_points(device_id, group_id).await {
        Ok(values) => Ok(Json(values)),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn update_point(
    Path((device_id, group_id, point_id)): Path<(u64, u64, u64)>,
    Json(update_point): Json<Value>,
) -> Result<(), AppError> {
    debug!("update_point:{:?}", update_point);
    match GLOBAL_DEVICE_MANAGER
        .update_point(device_id, group_id, point_id, update_point)
        .await
    {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

pub(crate) async fn delete_points(
    Path((device_id, group_id)): Path<(u64, u64)>,
    Query(query): Query<DeleteIdsQuery>,
) -> Result<(), AppError> {
    let point_ids: Vec<u64> = query
        .ids
        .split(',')
        .map(|s| s.parse::<u64>().map_err(AppError::new))
        .collect::<Result<Vec<u64>, _>>()?;
    match GLOBAL_DEVICE_MANAGER
        .delete_points(device_id, group_id, point_ids)
        .await
    {
        Ok(()) => Ok(()),
        Err(e) => Err(AppError::new(e)),
    }
}

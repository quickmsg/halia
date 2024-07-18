use axum::{
    extract::{Path, Query},
    Json,
};
use common::error::HaliaError;
use devices::modbus::manager::GLOBAL_MODBUS_MANAGER;
use types::devices::modbus::{
    CreateUpdateGroupPointReq, CreateUpdateGroupReq, CreateUpdateModbusReq,
    CreateUpdateSinkPointReq, CreateUpdateSinkReq, SearchGroupPointsResp, SearchGroupsResp,
    SearchSinkPointsResp, SearchSinksResp,
};
use uuid::Uuid;

use crate::{AppResp, DeleteIdsQuery, Pagination};

pub async fn create(Json(req): Json<CreateUpdateModbusReq>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.create(None, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn update(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateModbusReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.update(device_id, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn start(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.start(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn stop(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.stop(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn delete(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.delete(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn create_group(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateGroupReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .create_group(device_id, None, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn search_groups(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchGroupsResp> {
    match GLOBAL_MODBUS_MANAGER
        .search_groups(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(groups) => AppResp::with_data(groups),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_group(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .update_group(device_id, group_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_group(Path((device_id, group_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .delete_group(device_id, group_id)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn create_group_point(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupPointReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .create_group_point(device_id, group_id, None, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn search_group_points(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    pagination: Query<Pagination>,
) -> AppResp<SearchGroupPointsResp> {
    match GLOBAL_MODBUS_MANAGER
        .search_group_points(device_id, group_id, pagination.p, pagination.s)
        .await
    {
        Ok(values) => AppResp::with_data(values),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_group_point(
    Path((device_id, group_id, point_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupPointReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .update_group_point(device_id, group_id, point_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn write_group_point_value(
    Path((device_id, group_id, point_id)): Path<(Uuid, Uuid, Uuid)>,
    data: String,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .write_group_point_value(device_id, group_id, point_id, data)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn delete_group_points(
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

    match GLOBAL_MODBUS_MANAGER
        .delete_group_points(device_id, group_id, point_ids)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn create_sink(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .create_sink(device_id, None, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn search_sinks(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSinksResp> {
    match GLOBAL_MODBUS_MANAGER
        .search_sinks(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub async fn update_sink(
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .update_sink(device_id, sink_id, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn delete_sink(Path((device_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.delete_sink(device_id, sink_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn create_sink_point(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkPointReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .create_sink_point(device_id, group_id, None, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_sink_points(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSinkPointsResp> {
    match GLOBAL_MODBUS_MANAGER
        .search_sink_points(device_id, group_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_sink_point(
    Path((device_id, group_id, point_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkPointReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .update_sink_point(device_id, group_id, point_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_sink_points(
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

    match GLOBAL_MODBUS_MANAGER
        .delete_sink_points(device_id, group_id, point_ids)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

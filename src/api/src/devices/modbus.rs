use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use common::error::HaliaError;
use devices::modbus::manager::GLOBAL_MODBUS_MANAGER;
use types::devices::modbus::{
    CreateUpdateModbusReq, CreateUpdatePointReq, CreateUpdateSinkReq, SearchPointsResp,
    SearchSinksResp,
};
use uuid::Uuid;

use crate::{AppResp, DeleteIdsQuery, Pagination};

pub(crate) fn modbus_routes() -> Router {
    Router::new()
        .route("/", post(create))
        .route("/:device_id", put(update))
        .route("/:device_id/start", put(start))
        .route("/:device_id/stop", put(stop))
        .route("/:device_id", routing::delete(delete))
        .nest(
            "/:device_id",
            Router::new()
                .nest(
                    "/point",
                    Router::new()
                        .route("/", post(create_point))
                        .route("/", get(search_points))
                        .route("/:point_id", put(update_point))
                        .route("/:point_id/value", put(write_point_value))
                        .route("/", routing::delete(delete_points)),
                )
                .nest(
                    "/sink",
                    Router::new()
                        .route("/", post(create_sink))
                        .route("/", get(search_sinks))
                        .route("/:sink_id", put(update_sink))
                        .route("/:sink_id", routing::delete(delete_sink)),
                ),
        )
}

async fn create(Json(req): Json<CreateUpdateModbusReq>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.create(None, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn update(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateModbusReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.update(device_id, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn start(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.start(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn stop(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.stop(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.delete(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_point(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdatePointReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .create_point(device_id, None, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search_points(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchPointsResp> {
    match GLOBAL_MODBUS_MANAGER
        .search_points(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(values) => AppResp::with_data(values),
        Err(e) => e.into(),
    }
}

async fn update_point(
    Path((device_id, point_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdatePointReq>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .update_point(device_id, point_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn write_point_value(
    Path((device_id, point_id)): Path<(Uuid, Uuid)>,
    Json(value): Json<serde_json::Value>,
) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER
        .write_point_value(device_id, point_id, value)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete_points(
    Path(device_id): Path<Uuid>,
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
        .delete_points(device_id, point_ids)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_sink(
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

async fn search_sinks(
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

async fn update_sink(
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

async fn delete_sink(Path((device_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_MODBUS_MANAGER.delete_sink(device_id, sink_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

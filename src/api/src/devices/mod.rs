use axum::{
    extract::{Path, Query},
    routing::{delete, get, post, put},
    Json, Router,
};
use bytes::Bytes;
use devices::GLOBAL_DEVICE_MANAGER;
use types::{
    devices::{CreateUpdateDeviceReq, QueryParams, SearchDevicesResp, Summary},
    CreateUpdateSourceOrSinkReq, Pagination,
};
use uuid::Uuid;

use crate::{AppResult, AppSuccess};

// mod coap;
// mod modbus;
// mod opcua;

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_devices_summary))
        .route("/", post(create_device))
        .route("/", get(search_devices))
        .route("/:device_id", put(update_device))
        .route("/:device_id", put(update_device))
        .route("/:device_id", delete(delete_device))
        .nest(
            "/:device_id",
            Router::new()
                .nest(
                    "/source",
                    Router::new()
                        .route("/", post(create_source))
                        .route("/", get(search_sources))
                        .route("/:source_id", put(update_source))
                        .route("/:source_id", delete(delete_source)),
                )
                .nest(
                    "/sink",
                    Router::new()
                        .route("/", post(create_sink))
                        .route("/", get(search_sinks))
                        .route("/:sink_id", put(update_sink))
                        .route("/:sink_id", delete(delete_sink)),
                ),
        )
    // .nest("/modbus", modbus_routes())
    // .nest("/opcua", opcua_routes())
    // .nest("/coap", coap_routes())
}

async fn get_devices_summary() -> AppSuccess<Summary> {
    AppSuccess::data(GLOBAL_DEVICE_MANAGER.get_summary().await)
}

async fn create_device(Json(req): Json<CreateUpdateDeviceReq>) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER.create_device(None, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_devices(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchDevicesResp> {
    AppSuccess::data(
        GLOBAL_DEVICE_MANAGER
            .search_devices(pagination, query_params)
            .await,
    )
}

async fn update_device(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateDeviceReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER.update_device(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_device(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER.delete_device(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER
        .create_source(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(Path(device_id): Path<Uuid>, Json(req): Json<Bytes>) -> AppResult<()> {
    todo!()
}

async fn update_source(Path(device_id): Path<Uuid>, Json(req): Json<Bytes>) -> AppResult<()> {
    todo!()
}

async fn delete_source(Path(device_id): Path<Uuid>) -> AppResult<()> {
    todo!()
}

async fn create_sink(Json(req): Json<CreateUpdateSourceOrSinkReq>) -> AppResult<()> {
    todo!()
}

async fn search_sinks(Path(device_id): Path<Uuid>, Json(req): Json<Bytes>) -> AppResult<()> {
    todo!()
}

async fn update_sink(Path(device_id): Path<Uuid>, Json(req): Json<Bytes>) -> AppResult<()> {
    todo!()
}

async fn delete_sink(Path(device_id): Path<Uuid>) -> AppResult<()> {
    todo!()
}

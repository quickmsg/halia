use axum::{
    extract::{Path, Query},
    routing::{get, post, put},
    Json, Router,
};
use bytes::Bytes;
use devices::GLOBAL_DEVICE_MANAGER;
use types::{
    devices::{QueryParams, SearchDevicesResp, Summary},
    Pagination,
};
use uuid::Uuid;

use crate::{AppResult, AppSuccess};

mod coap;
mod modbus;
mod opcua;

pub fn routes() -> Router {
    Router::new()
        .route("/", post(create_device))
        .route("/", get(search_devices))
        .route("/summary", get(get_devices_summary))
        .route("/:device_id", put(update_device))
    // .nest("/modbus", modbus_routes())
    // .nest("/opcua", opcua_routes())
    // .nest("/coap", coap_routes())
}

async fn search_devices(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchDevicesResp> {
    AppSuccess::data(GLOBAL_DEVICE_MANAGER.search(pagination, query_params).await)
}

async fn get_devices_summary() -> AppSuccess<Summary> {
    AppSuccess::data(GLOBAL_DEVICE_MANAGER.get_summary().await)
}

async fn create_device(Json(req): Json<Bytes>) -> AppResult<()> {
    todo!()
}

async fn update_device(Path(device_id): Path<Uuid>, Json(req): Json<Bytes>) -> AppResult<()> {
    todo!()
}

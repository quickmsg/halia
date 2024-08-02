use axum::{extract::Query, routing::get, Router};
use coap::coap_routes;
use devices::GLOBAL_DEVICE_MANAGER;
use modbus::modbus_routes;
use opcua::opcua_routes;
use types::{devices::SearchDevicesResp, Pagination, QueryParams};

use crate::AppSuccess;

mod coap;
mod modbus;
mod opcua;

pub fn routes() -> Router {
    Router::new()
        .route("/", get(search_devices))
        .nest("/modbus", modbus_routes())
        .nest("/opcua", opcua_routes())
        .nest("/coap", coap_routes())
}

async fn search_devices(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchDevicesResp> {
    AppSuccess::data(GLOBAL_DEVICE_MANAGER.search(pagination, query_params).await)
}

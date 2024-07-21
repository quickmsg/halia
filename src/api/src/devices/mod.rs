use axum::{extract::Query, routing::get, Router};
// use coap::coap_routes;
use devices::GLOBAL_DEVICE_MANAGER;
use modbus::modbus_routes;
use opcua::opcua_routes;
use types::devices::SearchDevicesResp;

use crate::{AppResp, Pagination};

// mod coap;
mod modbus;
mod opcua;

pub fn routes() -> Router {
    Router::new()
        .route("/", get(search_devices))
        .nest("/modbus", modbus_routes())
        // .nest("/coap", coap_routes())
        .nest("/opcua", opcua_routes())
}

async fn search_devices(pagination: Query<Pagination>) -> AppResp<SearchDevicesResp> {
    AppResp::with_data(
        GLOBAL_DEVICE_MANAGER
            .search(pagination.p, pagination.s)
            .await,
    )
}

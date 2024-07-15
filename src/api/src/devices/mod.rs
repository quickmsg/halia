use axum::{
    extract::Query,
    routing::{delete, get, post, put},
    Router,
};
use device::GLOBAL_DEVICE_MANAGER;
use types::device::device::SearchDeviceResp;

use crate::{AppResp, Pagination};

mod modbus;

pub fn routes() -> Router {
    Router::new().nest(
        "/device",
        Router::new()
            .route("/", get(search_devices))
            .nest("/modbus", modbus_routes()),
    )
}

async fn search_devices(pagination: Query<Pagination>) -> AppResp<SearchDeviceResp> {
    AppResp::with_data(
        GLOBAL_DEVICE_MANAGER
            .search(pagination.p, pagination.s)
            .await,
    )
}

fn modbus_routes() -> Router {
    Router::new()
        .route("/", post(modbus::create))
        .route("/:device_id", put(modbus::update))
        .route("/:device_id/start", put(modbus::start))
        .route("/:device_id/stop", put(modbus::stop))
        .route("/:device_id", delete(modbus::delete))
        .nest(
            "/:device_id",
            Router::new()
                .nest(
                    "/group",
                    Router::new()
                        .route("/", post(modbus::create_group))
                        .route("/", get(modbus::search_groups))
                        .route("/:group_id", put(modbus::update_group))
                        .route("/:group_id", delete(modbus::delete_group))
                        .nest(
                            "/:group_id/point",
                            Router::new()
                                .route("/", post(modbus::create_group_point))
                                .route("/", get(modbus::search_group_points))
                                .route("/:point_id", put(modbus::update_group_point))
                                .route("/:point_id/value", post(modbus::write_group_point_value))
                                .route("/", delete(modbus::delete_group_points)),
                        ),
                )
                .nest(
                    "/sink",
                    Router::new()
                        .route("/", post(modbus::create_sink))
                        .route("/", get(modbus::search_sinks))
                        .route("/:sink_id", put(modbus::update_sink))
                        .route("/:sink_id", delete(modbus::delete_sink))
                        .nest(
                            "/:sink_id/point",
                            Router::new()
                                .route("/", post(modbus::create_sink_point))
                                .route("/", get(modbus::search_sink_points))
                                .route("/:point_id", put(modbus::update_sink_point))
                                .route("/", delete(modbus::delete_sink_points)),
                        ),
                ),
        )
}

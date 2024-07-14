use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use common::error::HaliaError;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};

mod apps;
mod device;
mod modbus;
mod rule;

#[derive(Serialize, Debug)]
pub(crate) struct AppResp<T> {
    code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
}

impl<T> AppResp<T> {
    pub fn new() -> Self {
        Self {
            code: 0,
            data: None,
        }
    }

    pub fn with_data(data: T) -> Self {
        Self {
            code: 0,
            data: Some(data),
        }
    }
}

impl<T> From<HaliaError> for AppResp<T> {
    fn from(err: HaliaError) -> Self {
        Self {
            code: err.code(),
            data: None,
        }
    }
}

impl<T: Serialize> IntoResponse for AppResp<T> {
    fn into_response(self) -> Response {
        (StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug, Deserialize)]
struct Pagination {
    p: usize,
    s: usize,
}

pub async fn start() {
    let app = Router::new()
        .nest("/api", device_routes())
        .nest("/api", app_routes())
        .nest("/api", rule_routes())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    let listener = TcpListener::bind("0.0.0.0:13000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize)]
pub(crate) struct DeleteIdsQuery {
    ids: String,
}

fn device_routes() -> Router {
    Router::new().nest(
        "/device",
        Router::new()
            .route("/", get(device::search_devices))
            .nest("/", modbus_routes()),
    )
}

fn app_routes() -> Router {
    Router::new().nest(
        "/app",
        Router::new()
            .route("/", post(apps::create_app))
            .route("/search", get(apps::search_apps))
            .route("/:app_id", put(apps::update_app))
            .route("/:app_id", delete(apps::delete_app))
            .nest(
                "/:app_id/source",
                Router::new()
                    .route("/", post(apps::create_source))
                    .route("/search", get(apps::search_sources))
                    .route("/:source_id", put(apps::update_source))
                    .route("/:source_id", delete(apps::delete_source)),
            )
            .nest(
                "/:app_id/sink",
                Router::new()
                    .route("/", post(apps::create_sink))
                    .route("/search", get(apps::search_sinks))
                    .route("/:sink_id", put(apps::update_sink))
                    .route("/:sink_id", delete(apps::delete_sink)),
            ),
    )
}

fn rule_routes() -> Router {
    Router::new()
        .route("/rule", post(rule::create))
        .route("/rules", get(rule::search))
        .route("/rule/:id", get(rule::read))
        .route("/rule/:id/start", put(rule::start))
        .route("/rule/:id/stop", put(rule::stop))
        .route("/rule/:id", put(rule::update))
        .route("/rule/:id", delete(rule::delete))
}

fn modbus_routes() -> Router {
    Router::new().nest(
        "/modbus",
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
                                    .route(
                                        "/:point_id/value",
                                        post(modbus::write_group_point_value),
                                    )
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
            ),
    )
}

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

mod device;
mod rule;
mod sink;
mod source;

#[derive(Serialize, Debug)]
pub(crate) struct AppResp<T> {
    code: u8,
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

pub async fn start() {
    let app = Router::new()
        .nest("/api", device_routes())
        .nest("/api/device/:device_id", group_routes())
        .nest("/api/device/:device_id/group/:group_id", point_routes())
        .nest("/api", source_routes())
        .nest("/api", sink_routes())
        .nest("/api", rule_routes())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize)]
pub(crate) struct DeleteIdsQuery {
    ids: String,
}

fn device_routes() -> Router {
    Router::new()
        .route("/device", post(device::create_device))
        .route("/device/:device_id", get(device::read_device))
        .route("/devices", get(device::read_devices))
        .route("/device/:device_id/start", put(device::start_device))
        .route("/device/:device_id/stop", put(device::stop_device))
        .route("/device/:device_id", put(device::update_device))
        .route("/device/:device_id", delete(device::delete_device))
}

fn group_routes() -> Router {
    Router::new()
        .route("/group", post(device::create_group))
        .route("/groups", get(device::read_groups))
        .route("/group/:group_id", put(device::update_group))
        .route("/groups", delete(device::delete_groups))
}

fn point_routes() -> Router {
    Router::new()
        .route("/points", post(device::create_points))
        .route("/points", get(device::read_points))
        .route("/point/:point_id", put(device::update_point))
        .route("/point/:point_id/value", put(device::write_point))
        .route("/points", delete(device::delete_points))
}

fn source_routes() -> Router {
    Router::new().route("/source", post(source::create_source))
}

fn sink_routes() -> Router {
    Router::new()
        .route("/sink", post(sink::create))
        .route("/sink/:id", get(sink::read))
        .route("/sinks", get(sink::list))
        .route("/update/:id", put(sink::update))
        .route("/delete/:id", delete(sink::delete))
}

fn rule_routes() -> Router {
    Router::new()
        .route("/graph", post(rule::create))
        // .route("/graph/run/:name", put(rule::run))
        .route("/graph/stop/:name", put(rule::stop))
        .route("/health", get(rule::helath))
}

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
mod mqtt;
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

#[derive(Debug, Deserialize)]
struct Pagination {
    p: u8,
    s: u8,
}

pub async fn start() {
    let app = Router::new()
        .nest("/api", device_routes())
        .nest("/api/device/:device_id", group_routes())
        .nest("/api/device/:device_id/group/:group_id", point_routes())
        .nest("/api/source", source_routes())
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
        .route("/device/search", get(device::search_device))
        .route("/device/:device_id/start", put(device::start_device))
        .route("/device/:device_id/stop", put(device::stop_device))
        .route("/device/:device_id", put(device::update_device))
        .route("/device/:device_id", delete(device::delete_device))
}

fn group_routes() -> Router {
    Router::new()
        .route("/group", post(device::create_group))
        .route("/group/search", get(device::search_group))
        .route("/group/:group_id", put(device::update_group))
        .route("/group/:group_id", delete(device::delete_group))
}

fn point_routes() -> Router {
    Router::new()
        .route("/point", post(device::create_point))
        .route("/point/search", get(device::search_point))
        .route("/point/:point_id", put(device::update_point))
        .route("/point/:point_id/value", put(device::write_point))
        .route("/points", delete(device::delete_points))
}

fn source_routes() -> Router {
    Router::new()
        .route("/source", post(source::create))
        .route("/source/search", get(source::search))
        .route("/source/:id", get(source::read))
        .route("/srouce/:id", put(source::update))
        .route("/srouce/:id", delete(source::delete))
        .nest("/:source_id/mqtt", mqtt_routes())
}

fn sink_routes() -> Router {
    Router::new()
        .route("/sink", post(sink::create))
        .route("/sinks", get(sink::list))
        .route("/sink/:id", get(sink::read))
        .route("/sink/:id", put(sink::update))
        .route("/sink/:id", delete(sink::delete))
}

fn rule_routes() -> Router {
    Router::new()
        .route("/rule", post(rule::create))
        .route("/rules", get(rule::list))
        .route("/rule/:id", get(rule::read))
        .route("/rule/:id/start", put(rule::start))
        .route("/rule/:id/stop", put(rule::stop))
        .route("/rule/:id", put(rule::update))
        .route("/rule/:id", delete(rule::delete))
}

// TODO
fn mqtt_routes() -> Router {
    Router::new()
        .route("/topic", post(mqtt::create))
        .route("/topic/search", todo!())
        .route("/topic/:topic_id", todo!())
        .route("/topic/:topic_id", todo!())
}

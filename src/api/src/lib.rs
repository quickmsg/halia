use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use common::error::HaliaError;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

mod device;
mod rule;

#[derive(Serialize, Debug)]
pub(crate) struct AppResp<T> {
    code: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
}

impl<T> AppResp<T> {
    pub fn new(data: T) -> Self {
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

#[derive(Serialize)]
pub(crate) struct AppError {
    error: String,
}

impl AppError {
    pub(crate) fn new<T: ToString>(error: T) -> Self {
        AppError {
            error: error.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, Json(self)).into_response()
    }
}

pub async fn start() {
    let app = Router::new()
        // 规则
        .route("/source", post(rule::create_source))
        .route("/sink", post(rule::create_sink))
        .route("/graph", post(rule::create))
        .route("/graph/run/:ame", put(rule::run))
        .route("/graph/stop/:name", put(rule::stop))
        .route("/health", get(rule::helath))
        // 设备
        .route("/device", post(device::create_device))
        .route("/device/:id", get(device::read_device))
        .route("/devices", get(device::read_devices))
        .route("/device/:id/start", put(device::start_device))
        .route("/device/:id/stop", put(device::stop_device))
        .route("/device/:id", put(device::update_device))
        .route("/device/:id", delete(device::delete_device))
        // 设备 -> 组
        .route("/device/:id/group", post(device::create_group))
        .route("/device/:id/groups", get(device::read_groups))
        .route(
            "/device/:device_id/group/:group_id",
            put(device::update_group),
        )
        .route("/device/:id/groups", delete(device::delete_groups))
        // 设备 -> 组 -> 点位
        .route(
            "/device/:device_id/group/:group_id/points",
            post(device::create_points),
        )
        .route(
            "/device/:device_id/group/:group_id/points",
            get(device::read_points),
        )
        .route(
            "/device/:device_id/group/:group_id/point/:point_id",
            put(device::update_point),
        )
        .route(
            "/device/:device_id/group/:group_id/points",
            delete(device::delete_points),
        );

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Deserialize)]
pub(crate) struct DeleteIdsQuery {
    ids: String,
}

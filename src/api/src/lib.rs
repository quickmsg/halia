use std::result;

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json, Router,
};
use common::error::HaliaError;
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};

mod app;
mod device;
mod rule;

pub(crate) type AppResult<T, E = AppError> = result::Result<T, E>;

#[derive(Serialize, Debug)]
pub(crate) struct AppSuccess<T> {
    code: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<T>,
}

impl<T> AppSuccess<T> {
    pub fn empty() -> Self {
        Self {
            code: 0,
            data: None,
        }
    }

    pub fn data(data: T) -> Self {
        Self {
            code: 0,
            data: Some(data),
        }
    }
}

impl<T: Serialize> IntoResponse for AppSuccess<T> {
    fn into_response(self) -> Response {
        (StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Serialize)]
pub(crate) struct AppError {
    code: u16,
    data: String,
}

impl AppError {
    pub fn new(e: String) -> Self {
        AppError { code: 1, data: e }
    }
}

impl From<HaliaError> for AppError {
    fn from(value: HaliaError) -> Self {
        Self {
            code: 1,
            data: value.to_string(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::OK, Json(self)).into_response()
    }
}

pub async fn start() {
    let app = Router::new()
        .nest("/api/device", device::routes())
        .nest("/api/app", app::routes())
        .nest("/api/rule", rule::rule_routes())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    let listener = TcpListener::bind("0.0.0.0:13000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
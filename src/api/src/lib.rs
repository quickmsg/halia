use std::result;

use ::apps::GLOBAL_APP_MANAGER;
use ::devices::GLOBAL_DEVICE_MANAGER;
use ::rule::GLOBAL_RULE_MANAGER;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use common::error::HaliaError;
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use types::Dashboard;

mod apps;
mod devices;
mod rule;

pub(crate) type AppResult<T, E = AppError> = result::Result<T, E>;

#[derive(Serialize, Debug)]
pub(crate) struct AppSuccess<T> {
    code: u8,
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
    data: Option<String>,
}

impl From<HaliaError> for AppError {
    fn from(value: HaliaError) -> Self {
        let code = value.code();
        match value {
            HaliaError::NotFound
            | HaliaError::NotFoundGroup
            | HaliaError::ProtocolNotSupported
            | HaliaError::ParseErr
            | HaliaError::IoErr
            | HaliaError::Existed
            | HaliaError::ConfErr
            | HaliaError::DeviceNotFound
            | HaliaError::DeviceRunning
            | HaliaError::DeviceStopped => Self { code, data: None },
            HaliaError::DeviceConnectionError(_) => todo!(),
            HaliaError::Common(s) => Self {
                code,
                data: Some(s),
            },
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
        .route("/api/dashboard", get(dashboard))
        .nest("/api/device", devices::routes())
        .nest("/api/app", apps::routes())
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

async fn dashboard() -> AppSuccess<Dashboard> {
    AppSuccess::data(Dashboard {
        device: GLOBAL_DEVICE_MANAGER.search_dashboard().await,
        app: GLOBAL_APP_MANAGER.search_dashboard().await,
        rule: GLOBAL_RULE_MANAGER.search_dashboard().await,
    })
}

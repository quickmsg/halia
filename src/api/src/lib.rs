use std::result;

use axum::{
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use common::{error::HaliaError, sys::get_machine_info};
use tokio::net::TcpListener;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
    trace::TraceLayer,
};
use types::Dashboard;
use user_api::auth;

mod app_api;
mod databoard_api;
mod device_api;
mod event_api;
mod rule_api;
mod schema_api;
mod user_api;

pub static EMPTY_USER_CODE: u16 = 2;
pub static WRONG_PASSWORD_CODE: u16 = 3;
pub static JWT_EXPIRED_CODE: u16 = 4;

pub(crate) type AppResult<T, E = AppError> = result::Result<T, E>;

pub(crate) struct AppError {
    code: StatusCode,
    data: String,
}

impl AppError {
    pub fn new(code: StatusCode, e: String) -> Self {
        AppError { code, data: e }
    }
}

impl From<HaliaError> for AppError {
    fn from(err: HaliaError) -> Self {
        match err {
            HaliaError::NotFound(e) => AppError::new(StatusCode::NOT_FOUND, e.to_string()),
            HaliaError::StorageErr(e) => {
                AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
            HaliaError::JsonErr(e) => {
                AppError::new(StatusCode::UNPROCESSABLE_ENTITY, e.to_string())
            }
            HaliaError::Common(e) => AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e),
            HaliaError::Io(io) => AppError::new(StatusCode::INTERNAL_SERVER_ERROR, io.to_string()),
            HaliaError::Running => todo!(),
            HaliaError::Stopped(e) => AppError::new(StatusCode::BAD_REQUEST, e),
            HaliaError::DeleteRefing => todo!(),
            HaliaError::DeleteRunning => todo!(),
            HaliaError::StopActiveRefing => AppError::new(
                StatusCode::BAD_REQUEST,
                "停止失败，有其他规则正在引用该规则".to_string(),
            ),
            HaliaError::NameExists => {
                AppError::new(StatusCode::BAD_REQUEST, "名称已存在！".to_string())
            }
            HaliaError::AddressExists => todo!(),
            HaliaError::Disconnect => todo!(),
            HaliaError::Error(e) => AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            HaliaError::NotSupportResource => todo!(),
            HaliaError::Base64DecodeErr(_) => todo!(),
            HaliaError::Form(e) => AppError::new(StatusCode::BAD_REQUEST, e),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.code, self.data).into_response()
    }
}

pub async fn start(port: u16) {
    let app = Router::new()
        .nest("/api", user_api::routes())
        .nest(
            "/api",
            Router::new()
                .route("/dashboard", get(get_dashboard))
                .nest("/device", device_api::routes())
                .nest("/app", app_api::routes())
                .nest("/databoard", databoard_api::routes())
                .nest("/rule", rule_api::routes())
                .nest("/event", event_api::routes())
                .nest("/schema", schema_api::routes())
                .route_layer(middleware::from_fn(auth)),
        )
        .fallback_service(
            ServeDir::new("./dist").not_found_service(ServeFile::new("./dist/index.html")),
        )
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );
        // .layer(TraceLayer::new_for_http());

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_dashboard() -> AppResult<Json<Dashboard>> {
    Ok(Json(Dashboard {
        machine_info: get_machine_info(),
        device_summary: devices::get_summary().await?,
        app_summary: apps::get_summary().await?,
        databoard_summary: databoard::get_summary().await?,
        rule_summary: rule::get_summary().await?,
    }))
}

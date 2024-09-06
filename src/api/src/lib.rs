use std::{result, sync::Arc};

use apps::App;
use axum::{
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use common::{error::HaliaError, sys::get_machine_info};
use databoard::databoard_struct::Databoard;
use devices::Device;
use rule::rule::Rule;
use serde::Serialize;
use sqlx::AnyPool;
use tokio::{net::TcpListener, sync::RwLock};
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};
use types::Dashboard;
use user_api::auth;

mod app_api;
mod databoard_api;
mod device_api;
mod rule_api;
mod user_api;

pub static EMPTY_USER_CODE: u16 = 2;
pub static WRONG_PASSWORD_CODE: u16 = 3;
pub static JWT_EXPIRED_CODE: u16 = 4;

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
    pub fn new(code: u16, e: String) -> Self {
        AppError { code, data: e }
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

#[derive(Clone)]
struct AppState {
    pool: Arc<AnyPool>,
    devices: Arc<RwLock<Vec<Box<dyn Device>>>>,
    apps: Arc<RwLock<Vec<Box<dyn App>>>>,
    databoards: Arc<RwLock<Vec<Databoard>>>,
    rules: Arc<RwLock<Vec<Rule>>>,
}

pub async fn start(
    port: u16,
    pool: Arc<AnyPool>,
    devices: Arc<RwLock<Vec<Box<dyn Device>>>>,
    apps: Arc<RwLock<Vec<Box<dyn App>>>>,
    databoards: Arc<RwLock<Vec<Databoard>>>,
    rules: Arc<RwLock<Vec<Rule>>>,
) {
    let state = AppState {
        pool,
        devices,
        apps,
        databoards,
        rules,
    };
    let app = Router::new()
        .with_state(state.clone())
        .nest("/api", user_api::routes())
        .nest(
            "/api",
            Router::new()
                .route("/dashboard", get(get_dashboard))
                .nest("/device", device_api::routes())
                .nest("/app", app_api::routes())
                .nest("/databoard", databoard_api::routes())
                .nest("/rule", rule_api::routes())
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

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();
    axum::serve(listener, app.with_state(state)).await.unwrap();
}

async fn get_dashboard() -> AppSuccess<Dashboard> {
    AppSuccess::data(Dashboard {
        machine_info: get_machine_info(),
        device_summary: devices::get_summary(),
        app_summary: apps::get_summary().await,
        databoard_summary: databoard::get_summary(),
        rule_summary: rule::get_summary(),
    })
}

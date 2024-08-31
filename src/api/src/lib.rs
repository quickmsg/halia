use std::{result, sync::Arc};

use ::databoard::databoard::Databoard;
use ::rule::rule::Rule;
use apps::App;
use axum::{
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    Json, Router,
};
use common::error::HaliaError;
use devices::Device;
use serde::Serialize;
use sqlx::AnyPool;
use tokio::{net::TcpListener, sync::RwLock};
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};
use user::auth;

mod app;
mod databoard;
mod device;
mod rule;
mod user;

pub static empty_user_code: u16 = 2;
pub static wrong_password_code: u16 = 3;
pub static jwt_expired_code: u16 = 4;

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
        .nest("/api", user::routes())
        .nest(
            "/",
            Router::new()
                .nest("/api/device", device::routes())
                .nest("/api/app", app::routes())
                .nest("/api/databoard", databoard::routes())
                .nest("/api/rule", rule::routes())
                .route_layer(middleware::from_fn_with_state(state.clone(), auth)),
        )
        // .fallback_service(
        //     ServeDir::new("./dist").not_found_service(ServeFile::new("./dist/index.html")),
        // )
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    let listener = TcpListener::bind("0.0.0.0:13000").await.unwrap();
    axum::serve(listener, app.with_state(state)).await.unwrap();
}

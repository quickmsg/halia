use std::{result, sync::Arc};

use apps::App;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json, Router,
};
use common::{error::HaliaError, persistence::local::Local};
use devices::Device;
use serde::Serialize;
use tokio::{
    net::TcpListener,
    sync::{Mutex, RwLock},
};
use tower_http::cors::{Any, CorsLayer};

mod app;
mod databoard;
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

// impl AppError {
//     pub fn new(e: String) -> Self {
//         AppError { code: 1, data: e }
//     }
// }

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
    persistence: Arc<Mutex<Local>>,
    devices: Arc<RwLock<Vec<Box<dyn Device>>>>,
    apps: Arc<RwLock<Vec<Box<dyn App>>>>,
    // rules: Arc<RwLock<Vec<Rule>>>,
}

pub async fn start(local_persistence: Local) {
    let state = AppState {
        persistence: Arc::new(Mutex::new(local_persistence)),
        devices: Arc::new(RwLock::new(vec![])),
        apps: Arc::new(RwLock::new(vec![])),
    };
    let app = Router::new()
        .with_state(state.clone())
        .nest("/api/device", device::routes())
        .nest("/api/app", app::routes())
        .nest("/api/rule", rule::routes())
        .nest("/api/databoard", databoard::routes())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    let listener = TcpListener::bind("0.0.0.0:13000").await.unwrap();
    axum::serve(listener, app.with_state(state)).await.unwrap();
}

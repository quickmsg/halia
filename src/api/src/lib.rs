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
mod devices;
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
        .nest("/api/device", devices::routes())
        .nest("/api/app", apps::routes())
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

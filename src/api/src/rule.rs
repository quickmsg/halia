use axum::debug_handler;
use axum::{extract::Path, http::StatusCode, Json};
use managers::graph::manager::GLOBAL_GRAPH_MANAGER;
use types::rule::{CreateGraph, ListRuleResp};
use uuid::Uuid;

use crate::AppResp;

pub(crate) async fn create(Json(req): Json<CreateGraph>) -> (StatusCode, String) {
    match GLOBAL_GRAPH_MANAGER.create(None, req).await {
        Ok(_) => return (StatusCode::CREATED, String::from("OK")),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                String::from(format!("because: {}", e)),
            )
        }
    };
}

pub(crate) async fn list() -> AppResp<Vec<ListRuleResp>> {
    match GLOBAL_GRAPH_MANAGER.list().await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

#[axum::debug_handler]
pub(crate) async fn start(Path(id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_GRAPH_MANAGER.start(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn stop(Path(_name): Path<String>) -> (StatusCode, String) {
    (StatusCode::OK, String::from("OK"))
}

pub(crate) async fn helath() -> (StatusCode, String) {
    return (StatusCode::CREATED, String::from("OK"));
}

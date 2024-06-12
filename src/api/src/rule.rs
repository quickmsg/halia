use axum::debug_handler;
use axum::{extract::Path, http::StatusCode, Json};
use managers::{graph::manager::GRAPH_MANAGER, sink::SINK_MANAGER};
use types::rule::{CreateGraph, CreateSink};

use crate::AppResp;

pub(crate) async fn create(Json(create_graph): Json<CreateGraph>) -> (StatusCode, String) {
    match GRAPH_MANAGER.lock().unwrap().register(create_graph) {
        Ok(_) => return (StatusCode::CREATED, String::from("OK")),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                String::from(format!("because: {}", e)),
            )
        }
    };
}

#[axum::debug_handler]
pub(crate) async fn run(Path(name): Path<String>) -> AppResp<()> {
    match GRAPH_MANAGER.lock().unwrap().run(name).await {
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


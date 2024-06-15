use axum::debug_handler;
use axum::{extract::Path, http::StatusCode, Json};
use rule::GLOBAL_RULE_MANAGER;
use types::rule::{CreateRuleReq, ListRuleResp};
use uuid::Uuid;

use crate::AppResp;

pub(crate) async fn create(Json(req): Json<CreateRuleReq>) -> (StatusCode, String) {
    match GLOBAL_RULE_MANAGER.create(None, req).await {
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
    match GLOBAL_RULE_MANAGER.list().await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

#[axum::debug_handler]
pub(crate) async fn start(Path(id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_RULE_MANAGER.start(id).await {
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

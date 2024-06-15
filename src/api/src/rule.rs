use axum::{extract::Path, Json};
use rule::GLOBAL_RULE_MANAGER;
use types::rule::{CreateRuleReq, ListRuleResp};
use uuid::Uuid;

use crate::AppResp;

pub(crate) async fn create(Json(req): Json<CreateRuleReq>) -> AppResp<()> {
    match GLOBAL_RULE_MANAGER.create(None, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn list() -> AppResp<Vec<ListRuleResp>> {
    match GLOBAL_RULE_MANAGER.list().await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn read(Path(id): Path<Uuid>) -> AppResp<CreateRuleReq> {
    match GLOBAL_RULE_MANAGER.read(id).await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn start(Path(id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_RULE_MANAGER.start(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn stop(Path(id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_RULE_MANAGER.stop(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update(Path(id): Path<Uuid>) -> AppResp<()> {
    // TODO
    match GLOBAL_RULE_MANAGER.stop(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete(Path(id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_RULE_MANAGER.delete(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use rule::GLOBAL_RULE_MANAGER;
use types::rules::{CreateUpdateRuleReq, SearchRulesResp};
use uuid::Uuid;

use crate::{AppResp, Pagination};

pub fn rule_routes() -> Router {
    Router::new().nest(
        "/rule",
        Router::new()
            .route("/", post(create))
            .route("/", get(search))
            .route("/:id/start", put(start))
            .route("/:id/stop", put(stop))
            .route("/:id", put(update))
            .route("/:id", routing::delete(delete)),
    )
}

async fn create(Json(req): Json<CreateUpdateRuleReq>) -> AppResp<()> {
    match GLOBAL_RULE_MANAGER.create(None, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search(pagination: Query<Pagination>) -> AppResp<SearchRulesResp> {
    match GLOBAL_RULE_MANAGER.search(pagination.p, pagination.s).await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

async fn start(Path(id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_RULE_MANAGER.start(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn stop(Path(id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_RULE_MANAGER.stop(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn update(Path(id): Path<Uuid>, Json(req): Json<CreateUpdateRuleReq>) -> AppResp<()> {
    // TODO
    match GLOBAL_RULE_MANAGER.stop(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete(Path(id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_RULE_MANAGER.delete(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

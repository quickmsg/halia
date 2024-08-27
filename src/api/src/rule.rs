use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use rule::GLOBAL_RULE_MANAGER;
use types::{
    rules::{CreateUpdateRuleReq, QueryParams, SearchRulesResp, Summary},
    Pagination,
};
use uuid::Uuid;

use crate::{AppResult, AppState, AppSuccess};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/summary", get(get_rules_summary))
        .route("/", post(create))
        .route("/", get(search))
        .route("/:id", put(update))
        .route("/:id/start", put(start))
        .route("/:id/stop", put(stop))
        .route("/:id", routing::delete(delete))
}

async fn get_rules_summary() -> AppSuccess<Summary> {
    AppSuccess::data(GLOBAL_RULE_MANAGER.get_summary().await)
}

async fn create(Json(req): Json<CreateUpdateRuleReq>) -> AppResult<AppSuccess<()>> {
    let rule_id = Uuid::new_v4();
    GLOBAL_RULE_MANAGER.create(rule_id, req, true).await?;
    Ok(AppSuccess::empty())
}

async fn search(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchRulesResp> {
    AppSuccess::data(GLOBAL_RULE_MANAGER.search(pagination, query_params).await)
}

async fn start(Path(id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_RULE_MANAGER.start(id).await?;
    Ok(AppSuccess::empty())
}

async fn stop(Path(id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_RULE_MANAGER.stop(id).await?;
    Ok(AppSuccess::empty())
}

async fn update(
    Path(id): Path<Uuid>,
    Json(req): Json<CreateUpdateRuleReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_RULE_MANAGER.update(id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete(Path(id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_RULE_MANAGER.delete(id).await?;
    Ok(AppSuccess::empty())
}

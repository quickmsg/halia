use axum::{
    extract::{Path, Query, State},
    routing::{self, get, post, put},
    Router,
};
use types::{
    rules::{QueryParams, SearchRulesResp, Summary},
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

async fn get_rules_summary(State(state): State<AppState>) -> AppSuccess<Summary> {
    let summary = rule::get_summary(&state.rules).await;
    AppSuccess::data(summary)
}

async fn create(State(state): State<AppState>, body: String) -> AppResult<AppSuccess<()>> {
    rule::create(
        &state.rules,
        &state.devices,
        &state.apps,
        &state.persistence,
        Uuid::new_v4(),
        body,
        true,
    )
    .await?;
    Ok(AppSuccess::empty())
}

async fn search(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchRulesResp> {
    let rules = rule::search(&state.rules, pagination, query_params).await;
    AppSuccess::data(rules)
}

async fn start(State(state): State<AppState>, Path(id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    rule::start(
        &state.rules,
        &state.devices,
        &state.apps,
        &state.persistence,
        id,
    )
    .await?;
    Ok(AppSuccess::empty())
}

async fn stop(State(state): State<AppState>, Path(id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    rule::stop(&state.rules, &state.persistence, id).await?;
    Ok(AppSuccess::empty())
}

async fn update(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    rule::update(&state.rules, &state.persistence, id, body).await?;
    Ok(AppSuccess::empty())
}

async fn delete(State(state): State<AppState>, Path(id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    rule::delete(&state.rules, &state.persistence, id).await?;
    Ok(AppSuccess::empty())
}

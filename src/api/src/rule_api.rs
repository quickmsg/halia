use std::convert::Infallible;

use axum::{
    extract::{Path, Query},
    response::{sse::Event, IntoResponse, Sse},
    routing::{self, get, post, put},
    Json, Router,
};
use futures_util::Stream;
use types::{
    rules::{CreateUpdateRuleReq, QueryParams, ReadRuleNodeResp, SearchRulesResp, Summary},
    Pagination,
};

use crate::{AppResult, AppSuccess};

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_rules_summary))
        .route("/", post(create))
        .route("/", get(search))
        .route("/:id", get(read))
        .route("/:id", put(update))
        .route("/:id/start", put(start))
        .route("/:id/stop", put(stop))
        .route("/:id", routing::delete(delete))
        .route("/:id/log/sse", get(sse_log))
        .route("/:id/log/download", get(download_log))
        .route("/:id/log/start", put(start_log))
        .route("/:id/log/stop", put(stop_log))
}

async fn get_rules_summary() -> AppSuccess<Summary> {
    AppSuccess::data(rule::get_summary())
}

async fn create(Json(req): Json<CreateUpdateRuleReq>) -> AppResult<AppSuccess<()>> {
    rule::create(req).await?;
    Ok(AppSuccess::empty())
}

async fn search(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchRulesResp>> {
    let resp = rule::search(pagination, query_params).await?;
    Ok(AppSuccess::data(resp))
}

// TODO
async fn read(Path(id): Path<String>) -> AppResult<AppSuccess<Vec<ReadRuleNodeResp>>> {
    let resp = rule::read(id).await?;
    Ok(AppSuccess::data(resp))
}

async fn start(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    rule::start(id).await?;
    Ok(AppSuccess::empty())
}

async fn stop(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    rule::stop(id).await?;
    Ok(AppSuccess::empty())
}

async fn sse_log(
    Path(id): Path<String>,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    match common::log::tail_log(&id).await {
        Ok(sse) => Ok(sse),
        Err(e) => Err(crate::AppError {
            code: 1,
            data: e.to_string(),
        }),
    }
}

async fn download_log(Path(id): Path<String>) -> impl IntoResponse {
    common::log::download_log(&id).await
}

async fn update(
    Path(id): Path<String>,
    Json(req): Json<CreateUpdateRuleReq>,
) -> AppResult<AppSuccess<()>> {
    rule::update(id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start_log(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    rule::start_log(id).await?;
    Ok(AppSuccess::empty())
}

async fn stop_log(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    rule::stop_log(id).await?;
    Ok(AppSuccess::empty())
}

async fn delete(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    rule::delete(id).await?;
    Ok(AppSuccess::empty())
}

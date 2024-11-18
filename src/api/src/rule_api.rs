use std::convert::Infallible;

use axum::{
    extract::{Path, Query},
    http::StatusCode,
    response::{sse::Event, IntoResponse, Sse},
    routing::{self, get, post, put},
    Json, Router,
};
use futures_util::Stream;
use types::{
    rules::{
        CreateUpdateRuleReq, ListRulesResp, QueryParams, ReadRuleNodeResp, ReadRuleResp, Summary,
    },
    Pagination,
};

use crate::AppResult;

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_rules_summary))
        .route("/", post(create))
        .route("/list", get(list_rules))
        .route("/:id", get(read))
        .route("/:id", put(update))
        .route("/:id/start", put(start))
        .route("/:id/stop", put(stop))
        .route("/:id", routing::delete(delete))
        .route("/:id/log", get(sse_log))
        .route("/:id/log/download", get(download_log))
        .route("/:id/log/start", put(start_log))
        .route("/:id/log/stop", put(stop_log))
        .route("/:id/log", routing::delete(delete_log))
}

async fn get_rules_summary() -> AppResult<Json<Summary>> {
    Ok(Json(rule::get_summary()))
}

async fn create(Json(req): Json<CreateUpdateRuleReq>) -> AppResult<()> {
    rule::create(req).await?;
    Ok(())
}

async fn list_rules(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<Json<ListRulesResp>> {
    let resp = rule::list(pagination, query_params).await?;
    Ok(Json(resp))
}

async fn read(Path(id): Path<String>) -> AppResult<Json<ReadRuleResp>> {
    let resp = rule::read(id).await?;
    Ok(Json(resp))
}

async fn start(Path(id): Path<String>) -> AppResult<()> {
    rule::start(id).await?;
    Ok(())
}

async fn stop(Path(id): Path<String>) -> AppResult<()> {
    rule::stop(id).await?;
    Ok(())
}

async fn sse_log(
    Path(id): Path<String>,
) -> AppResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    match common::log::tail_log(&id).await {
        Ok(sse) => Ok(sse),
        Err(e) => Err(crate::AppError {
            code: StatusCode::INTERNAL_SERVER_ERROR,
            data: e.to_string(),
        }),
    }
}

async fn delete_log(Path(id): Path<String>) -> AppResult<()> {
    common::log::delete_log(&id).await;
    Ok(())
}

async fn download_log(Path(id): Path<String>) -> impl IntoResponse {
    common::log::download_log(&id).await
}

async fn update(Path(id): Path<String>, Json(req): Json<CreateUpdateRuleReq>) -> AppResult<()> {
    rule::update(id, req).await?;
    Ok(())
}

async fn start_log(Path(id): Path<String>) -> AppResult<()> {
    rule::start_log(id).await?;
    Ok(())
}

async fn stop_log(Path(id): Path<String>) -> AppResult<()> {
    rule::stop_log(id).await?;
    Ok(())
}

async fn delete(Path(id): Path<String>) -> AppResult<()> {
    rule::delete(id).await?;
    Ok(())
}

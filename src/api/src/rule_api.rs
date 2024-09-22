use axum::{
    body::Body,
    extract::{Path, Query},
    http::{header, StatusCode},
    response::IntoResponse,
    routing::{self, get, post, put},
    Json, Router,
};
use tokio_util::io::ReaderStream;
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
        .route("/:id/log/download", get(download_log))
        .route("/:id", routing::delete(delete))
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
) -> AppSuccess<SearchRulesResp> {
    let rules = rule::search(pagination, query_params).await;
    AppSuccess::data(rules)
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

async fn download_log(Path(id): Path<String>) -> impl IntoResponse {
    let filename = match rule::get_log_filename(id).await {
        Ok(filename) => filename,
        Err(_) => return Err((StatusCode::NOT_FOUND, format!("规则 not found"))),
    };

    let file = match tokio::fs::File::open(filename).await {
        Ok(file) => file,
        Err(err) => return Err((StatusCode::NOT_FOUND, format!("File not found: {}", err))),
    };

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let headers = [
        (header::CONTENT_TYPE, "text/plain; charset=utf-8"),
        (
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"download_file.txt\"",
        ),
    ];

    Ok((headers, body))
}

async fn update(
    Path(id): Path<String>,
    Json(req): Json<CreateUpdateRuleReq>,
) -> AppResult<AppSuccess<()>> {
    rule::update(id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    rule::delete(id).await?;
    Ok(AppSuccess::empty())
}

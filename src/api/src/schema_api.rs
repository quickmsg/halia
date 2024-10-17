use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use types::{
    schema::{CreateUpdateSchemaReq, QueryParams, SearchSchemasResp},
    Pagination,
};

use crate::{AppResult, AppSuccess};

pub fn routes() -> Router {
    Router::new()
        .route("/", post(create))
        .route("/", get(search))
        .route("/:id", put(update))
        .route("/:id", routing::delete(delete))
}

async fn create(Json(req): Json<CreateUpdateSchemaReq>) -> AppResult<AppSuccess<()>> {
    schema::create(req).await?;
    Ok(AppSuccess::empty())
}

async fn search(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSchemasResp>> {
    let resp = schema::search(pagination, query_params).await?;
    Ok(AppSuccess::data(resp))
}

async fn update(
    Path(id): Path<String>,
    Json(req): Json<CreateUpdateSchemaReq>,
) -> AppResult<AppSuccess<()>> {
    schema::update(id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    schema::delete(id).await?;
    Ok(AppSuccess::empty())
}

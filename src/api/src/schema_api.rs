use axum::{
    extract::{Path, Query},
    routing::{delete, get, post, put},
    Json, Router,
};
use types::{
    schema::{CreateUpdateSchemaReq, QueryParams, SearchSchemasResp},
    Pagination,
};

use crate::AppResult;

pub fn routes() -> Router {
    Router::new()
        .route("/", post(create_schema))
        .route("/list", get(list_schemas))
        .route("/:id", put(update_schema))
        .route("/:id", delete(delete_schema))
}

async fn create_schema(Json(req): Json<CreateUpdateSchemaReq>) -> AppResult<()> {
    schema::create(req).await?;
    Ok(())
}

async fn list_schemas(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<Json<SearchSchemasResp>> {
    let resp = schema::search(pagination, query_params).await?;
    Ok(Json(resp))
}

async fn update_schema(
    Path(id): Path<String>,
    Json(req): Json<CreateUpdateSchemaReq>,
) -> AppResult<()> {
    schema::update(id, req).await?;
    Ok(())
}

async fn delete_schema(Path(id): Path<String>) -> AppResult<()> {
    schema::delete(id).await?;
    Ok(())
}

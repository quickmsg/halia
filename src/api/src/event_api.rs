use axum::{extract::Query, routing::get, Json, Router};
use types::{
    events::{QueryParams, SearchEventsResp},
    Pagination,
};

use crate::AppResult;

pub fn routes() -> Router {
    Router::new().route("/list", get(list_events))
}

async fn list_events(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<Json<SearchEventsResp>> {
    let resp = events::search_events(query_params, pagination).await?;
    Ok(Json(resp))
}

use axum::{extract::Query, routing::get, Router};
use types::{
    events::{QueryParams, SearchEventsResp},
    Pagination,
};

use crate::{AppResult, AppSuccess};

pub fn routes() -> Router {
    Router::new().route("/", get(search_events))
}

async fn search_events(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchEventsResp>> {
    let data = events::search_events(query_params, pagination).await?;
    Ok(AppSuccess::data(data))
}

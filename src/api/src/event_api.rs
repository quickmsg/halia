use axum::{extract::Query, routing::get, Router};
use types::{
    events::{QueryParams, SearchEventsResp},
    Pagination,
};

use crate::AppSuccess;

pub fn routes() -> Router {
    Router::new().route("/", get(search_events))
}

async fn search_events(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchEventsResp> {
    // AppSuccess::data(events::search_events(&state.pool, query_params, pagination).await)
    todo!()
}

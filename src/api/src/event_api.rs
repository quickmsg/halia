use axum::{
    extract::{Query, State},
    routing::get,
    Router,
};
use types::{
    events::{QueryParams, SearchEventsResp},
    Pagination,
};

use crate::{AppState, AppSuccess};

pub fn routes() -> Router<AppState> {
    Router::new().route("/", get(search_events))
}

async fn search_events(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchEventsResp> {
    // AppSuccess::data(events::search_events(&state.pool, query_params, pagination).await)
    todo!()
}

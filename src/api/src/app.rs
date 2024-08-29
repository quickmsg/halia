use axum::{
    extract::{Path, Query, State},
    routing::{self, get, post, put},
    Router,
};
use types::{
    apps::{QueryParams, SearchAppsResp, Summary},
    Pagination, SearchSourcesOrSinksResp,
};
use uuid::Uuid;

use crate::{AppResult, AppState, AppSuccess};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/summary", get(get_apps_summary))
        .route("/", post(create_app))
        .route("/", get(search_apps))
        .route("/:app_id", put(update_app))
        .route("/:app_id/start", put(start_app))
        .route("/:app_id/stop", put(stop_app))
        .route("/:app_id", routing::delete(delete_app))
        .nest(
            "/:app_id",
            Router::new()
                .nest(
                    "/source",
                    Router::new()
                        .route("/", post(create_source))
                        .route("/", get(search_sources))
                        .route("/:source_id", put(update_source))
                        .route("/:source_id", routing::delete(delete_source)),
                )
                .nest(
                    "/sink",
                    Router::new()
                        .route("/", post(create_sink))
                        .route("/", get(search_sinks))
                        .route("/:sink_id", put(update_sink))
                        .route("/:sink_id", routing::delete(delete_sink)),
                ),
        )
}

async fn get_apps_summary(State(state): State<AppState>) -> AppSuccess<Summary> {
    let summary = apps::get_summary(&state.apps).await;
    AppSuccess::data(summary)
}

async fn create_app(State(state): State<AppState>, body: String) -> AppResult<AppSuccess<()>> {
    apps::create_app(&state.pool, &state.apps, Uuid::new_v4(), body, true).await?;
    Ok(AppSuccess::empty())
}

async fn search_apps(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppSuccess<SearchAppsResp> {
    let apps = apps::search_apps(&state.apps, pagination, query).await;
    AppSuccess::data(apps)
}

async fn update_app(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    apps::update_app(&state.pool, &state.apps, app_id, body).await?;
    Ok(AppSuccess::empty())
}

async fn start_app(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    apps::start_app(&state.pool, &state.apps, app_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop_app(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    apps::stop_app(&state.pool, &state.apps, app_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete_app(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    apps::delete_app(&state.pool, &state.apps, app_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    apps::create_source(&state.pool, &state.apps, app_id, Uuid::new_v4(), body, true).await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sources = apps::search_sources(&state.apps, app_id, pagination, query).await?;
    Ok(AppSuccess::data(sources))
}

async fn update_source(
    State(state): State<AppState>,
    Path((app_id, source_id)): Path<(Uuid, Uuid)>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    apps::update_source(&state.pool, &state.apps, app_id, source_id, body).await?;
    Ok(AppSuccess::empty())
}

async fn delete_source(
    State(state): State<AppState>,
    Path((app_id, source_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    apps::delete_source(&state.pool, &state.apps, app_id, source_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    apps::create_sink(&state.pool, &state.apps, app_id, Uuid::new_v4(), body, true).await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sinks = apps::search_sinks(&state.apps, app_id, pagination, query).await?;
    Ok(AppSuccess::data(sinks))
}

async fn update_sink(
    State(state): State<AppState>,
    Path((app_id, sink_id)): Path<(Uuid, Uuid)>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    apps::update_sink(&state.pool, &state.apps, app_id, sink_id, body).await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(
    State(state): State<AppState>,
    Path((app_id, sink_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    apps::delete_sink(&state.pool, &state.apps, app_id, sink_id).await?;
    Ok(AppSuccess::empty())
}

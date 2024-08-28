use apps::GLOBAL_APP_MANAGER;
use axum::{
    extract::{Path, Query, State},
    routing::{self, get, post, put},
    Json, Router,
};
use types::{
    apps::{QueryParams, SearchAppsResp, Summary},
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp,
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
    apps::create_app(&state.apps, &state.persistence, Uuid::new_v4(), body, true).await?;
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
    apps::update_app(&state.apps, &state.persistence, app_id, body).await?;
    Ok(AppSuccess::empty())
}

async fn start_app(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    apps::start_app(&state.apps, &state.persistence, app_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop_app(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    apps::stop_app(&state.apps, &state.persistence, app_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete_app(
    State(state): State<AppState>,
    Path(app_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    apps::delete_app(&state.apps, &state.persistence, app_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    let source_id = Uuid::new_v4();
    GLOBAL_APP_MANAGER
        .create_source(app_id, source_id, req, true)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let data = GLOBAL_APP_MANAGER
        .search_sources(app_id, pagination, query)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_source(
    Path((app_id, source_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER
        .update_source(app_id, source_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_source(Path((app_id, source_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.delete_source(app_id, source_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    let sink_id = Uuid::new_v4();
    GLOBAL_APP_MANAGER
        .create_sink(app_id, sink_id, req, true)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let data = GLOBAL_APP_MANAGER
        .search_sinks(app_id, pagination, query)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_sink(
    Path((app_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.update_sink(app_id, sink_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(Path((app_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.delete_sink(app_id, sink_id).await?;
    Ok(AppSuccess::empty())
}

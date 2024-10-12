use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use tracing::debug;
use types::{
    apps::{
        CreateUpdateAppReq, QueryParams, QueryRuleInfo, SearchAppsResp, SearchRuleInfo, Summary,
    },
    CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams, SearchSourcesOrSinksResp,
};

use crate::{AppResult, AppSuccess};

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_apps_summary))
        .route("/rule", get(get_rule_info))
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

async fn get_apps_summary() -> AppSuccess<Summary> {
    AppSuccess::data(apps::get_summary().await)
}

async fn get_rule_info(
    Query(query): Query<QueryRuleInfo>,
) -> AppResult<AppSuccess<SearchRuleInfo>> {
    let rule_info = apps::get_rule_info(query).await?;
    Ok(AppSuccess::data(rule_info))
}

async fn create_app(Json(req): Json<CreateUpdateAppReq>) -> AppResult<AppSuccess<()>> {
    debug!("here");
    apps::create_app(req).await?;
    debug!("here");
    Ok(AppSuccess::empty())
}

async fn search_apps(
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchAppsResp>> {
    let apps = apps::search_apps(pagination, query).await?;
    Ok(AppSuccess::data(apps))
}

async fn update_app(
    Path(app_id): Path<String>,
    Json(req): Json<CreateUpdateAppReq>,
) -> AppResult<AppSuccess<()>> {
    apps::update_app(app_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start_app(Path(app_id): Path<String>) -> AppResult<AppSuccess<()>> {
    apps::start_app(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop_app(Path(app_id): Path<String>) -> AppResult<AppSuccess<()>> {
    apps::stop_app(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete_app(Path(app_id): Path<String>) -> AppResult<AppSuccess<()>> {
    apps::delete_app(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    Path(app_id): Path<String>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    apps::create_source(app_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(
    Path(app_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QuerySourcesOrSinksParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sources = apps::search_sources(app_id, pagination, query).await?;
    Ok(AppSuccess::data(sources))
}

async fn update_source(
    Path((app_id, source_id)): Path<(String, String)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    apps::update_source(app_id, source_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_source(
    Path((app_id, source_id)): Path<(String, String)>,
) -> AppResult<AppSuccess<()>> {
    apps::delete_source(app_id, source_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    Path(app_id): Path<String>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    apps::create_sink(app_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    Path(app_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QuerySourcesOrSinksParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sinks = apps::search_sinks(app_id, pagination, query).await?;
    Ok(AppSuccess::data(sinks))
}

async fn update_sink(
    Path((app_id, sink_id)): Path<(String, String)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    apps::update_sink(app_id, sink_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(Path((app_id, sink_id)): Path<(String, String)>) -> AppResult<AppSuccess<()>> {
    apps::delete_sink(app_id, sink_id).await?;
    Ok(AppSuccess::empty())
}

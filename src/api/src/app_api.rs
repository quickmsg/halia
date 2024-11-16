use axum::{
    extract::{Path, Query},
    routing::{delete, get, post, put},
    Json, Router,
};
use types::{
    apps::{CreateAppReq, ListAppsResp, QueryParams, ReadAppResp, Summary, UpdateAppReq},
    rules::ListRulesResp,
    CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams, SearchSourcesOrSinksResp,
};

use crate::AppResult;

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_apps_summary))
        // .route("/rule", get(get_rule_info))
        .route("/", post(create_app))
        .route("/list", get(list_apps))
        .route("/:app_id", get(read_app))
        .route("/:app_id", put(update_app))
        .route("/:app_id/start", put(start_app))
        .route("/:app_id/stop", put(stop_app))
        .route("/:app_id", delete(delete_app))
        .nest(
            "/:app_id",
            Router::new()
                .route("/rule/list", get(list_rules))
                .nest(
                    "/source",
                    Router::new()
                        .route("/", post(create_source))
                        .route("/list", get(list_sources))
                        .route("/:source_id", put(update_source))
                        .route("/:source_id", delete(delete_source)),
                )
                .nest(
                    "/sink",
                    Router::new()
                        .route("/", post(create_sink))
                        .route("/", get(list_sinks))
                        .route("/:sink_id", put(update_sink))
                        .route("/:sink_id", delete(delete_sink)),
                ),
        )
}

async fn get_apps_summary() -> AppResult<Json<Summary>> {
    let summary = apps::get_summary().await;
    Ok(Json(summary))
}

// async fn get_rule_info(Query(query): Query<QueryRuleInfo>) -> AppResult<Json<SearchRuleInfo>> {
//     let rule_info = apps::get_rule_info(query).await?;
//     Ok(Json(rule_info))
// }

async fn create_app(Json(req): Json<CreateAppReq>) -> AppResult<()> {
    apps::create_app(req).await?;
    Ok(())
}

async fn list_apps(
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<Json<ListAppsResp>> {
    let resp = apps::list_apps(pagination, query).await?;
    Ok(Json(resp))
}

async fn read_app(Path(app_id): Path<String>) -> AppResult<Json<ReadAppResp>> {
    let result = apps::read_app(app_id).await?;
    Ok(Json(result))
}

async fn update_app(Path(app_id): Path<String>, Json(req): Json<UpdateAppReq>) -> AppResult<()> {
    apps::update_app(app_id, req).await?;
    Ok(())
}

async fn start_app(Path(app_id): Path<String>) -> AppResult<()> {
    apps::start_app(app_id).await?;
    Ok(())
}

async fn stop_app(Path(app_id): Path<String>) -> AppResult<()> {
    apps::stop_app(app_id).await?;
    Ok(())
}

async fn delete_app(Path(app_id): Path<String>) -> AppResult<()> {
    apps::delete_app(app_id).await?;
    Ok(())
}

async fn list_rules(
    Path(app_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::rules::QueryParams>,
) -> AppResult<Json<ListRulesResp>> {
    let resp = apps::list_rules(app_id, pagination, query).await?;
    Ok(Json(resp))
}

async fn create_source(
    Path(app_id): Path<String>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<()> {
    apps::create_source(app_id, req).await?;
    Ok(())
}

async fn list_sources(
    Path(app_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QuerySourcesOrSinksParams>,
) -> AppResult<Json<SearchSourcesOrSinksResp>> {
    let sources = apps::search_sources(app_id, pagination, query).await?;
    Ok(Json(sources))
}

async fn update_source(
    Path((app_id, source_id)): Path<(String, String)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<()> {
    apps::update_source(app_id, source_id, req).await?;
    Ok(())
}

async fn delete_source(Path((app_id, source_id)): Path<(String, String)>) -> AppResult<()> {
    apps::delete_source(app_id, source_id).await?;
    Ok(())
}

async fn create_sink(
    Path(app_id): Path<String>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<()> {
    apps::create_sink(app_id, req).await?;
    Ok(())
}

async fn list_sinks(
    Path(app_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QuerySourcesOrSinksParams>,
) -> AppResult<Json<SearchSourcesOrSinksResp>> {
    let sinks = apps::search_sinks(app_id, pagination, query).await?;
    Ok(Json(sinks))
}

async fn update_sink(
    Path((app_id, sink_id)): Path<(String, String)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<()> {
    apps::update_sink(app_id, sink_id, req).await?;
    Ok(())
}

async fn delete_sink(Path((app_id, sink_id)): Path<(String, String)>) -> AppResult<()> {
    apps::delete_sink(app_id, sink_id).await?;
    Ok(())
}

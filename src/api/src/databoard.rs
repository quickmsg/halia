use apps::GLOBAL_APP_MANAGER;
use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use types::{
    apps::{CreateUpdateAppReq, QueryParams, SearchAppsResp, Summary},
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp,
};
use uuid::Uuid;

use crate::{AppResult, AppSuccess};

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_databoards_summary))
        .route("/", post(create_databoard))
        .route("/", get(search_databoards))
        .route("/:databoard_id", put(update_databoard))
        .route("/:databoard_id", routing::delete(delete_databoard))
        .nest(
            "/:databoard_id/data",
            Router::new()
                .route("/", post(create_data))
                .route("/", get(search_datas))
                .route("/:data_id", put(update_data))
                .route("/:data_id", routing::delete(delete_data)),
        )
}

async fn get_databoards_summary() -> AppSuccess<Summary> {
    todo!()
}

async fn create_databoard(Json(req): Json<CreateUpdateAppReq>) -> AppResult<AppSuccess<()>> {
    let app_id = Uuid::new_v4();
    GLOBAL_APP_MANAGER.create_app(app_id, req, true).await?;
    Ok(AppSuccess::empty())
}

async fn search_databoards(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchAppsResp> {
    AppSuccess::data(
        GLOBAL_APP_MANAGER
            .search_apps(pagination, query_params)
            .await,
    )
}

async fn update_databoard(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateAppReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.update_app(app_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_databoard(Path(app_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.delete_app(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_data(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    let source_id = Uuid::new_v4();
    GLOBAL_APP_MANAGER
        .create_source(app_id, source_id, req, true)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_datas(
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let data = GLOBAL_APP_MANAGER
        .search_sources(app_id, pagination, query)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_data(
    Path((app_id, source_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER
        .update_source(app_id, source_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_data(Path((app_id, source_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.delete_source(app_id, source_id).await?;
    Ok(AppSuccess::empty())
}

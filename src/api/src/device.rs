use axum::{
    extract::{Path, Query},
    routing::{delete, get, post, put},
    Json, Router,
};
use devices::GLOBAL_DEVICE_MANAGER;
use types::{
    devices::{CreateUpdateDeviceReq, QueryParams, SearchDevicesResp, Summary},
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp, Value,
};
use uuid::Uuid;

use crate::{AppResult, AppSuccess};

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_devices_summary))
        .route("/", post(create_device))
        .route("/", get(search_devices))
        .route("/:device_id", put(update_device))
        .route("/:device_id/start", put(start_device))
        .route("/:device_id/stop", put(stop_device))
        .route("/:device_id", delete(delete_device))
        .nest(
            "/:device_id",
            Router::new()
                .nest(
                    "/source",
                    Router::new()
                        .route("/", post(create_source))
                        .route("/", get(search_sources))
                        .route("/:source_id", put(update_source))
                        .route("/:source_id/value", put(write_source_value))
                        .route("/:source_id", delete(delete_source)),
                )
                .nest(
                    "/sink",
                    Router::new()
                        .route("/", post(create_sink))
                        .route("/", get(search_sinks))
                        .route("/:sink_id", put(update_sink))
                        .route("/:sink_id", delete(delete_sink)),
                ),
        )
}

async fn get_devices_summary() -> AppSuccess<Summary> {
    AppSuccess::data(GLOBAL_DEVICE_MANAGER.get_summary().await)
}

async fn create_device(Json(req): Json<CreateUpdateDeviceReq>) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER.create_device(None, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_devices(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchDevicesResp> {
    AppSuccess::data(
        GLOBAL_DEVICE_MANAGER
            .search_devices(pagination, query_params)
            .await,
    )
}

async fn update_device(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateDeviceReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER.update_device(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start_device(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER.start_device(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop_device(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER.stop_device(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete_device(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER.delete_device(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER
        .create_source(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sources = GLOBAL_DEVICE_MANAGER
        .search_sources(device_id, pagination, query_params)
        .await?;
    Ok(AppSuccess::data(sources))
}

async fn update_source(
    Path((device_id, source_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER
        .update_source(device_id, source_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn write_source_value(
    Path((device_id, source_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<Value>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER
        .write_source_value(device_id, source_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_source(
    Path((device_id, source_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER
        .delete_source(device_id, source_id)
        .await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER.create_sink(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sinks = GLOBAL_DEVICE_MANAGER
        .search_sinks(device_id, pagination, query)
        .await?;
    Ok(AppSuccess::data(sinks))
}

async fn update_sink(
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER
        .update_sink(device_id, sink_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(Path((device_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_DEVICE_MANAGER
        .delete_sink(device_id, sink_id)
        .await?;
    Ok(AppSuccess::empty())
}

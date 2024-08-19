use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use devices::coap::manager::GLOBAL_COAP_MANAGER;
use types::{
    devices::{
        coap::{
            CreateUpdateAPIReq, CreateUpdateCoapReq, CreateUpdateObserveReq, CreateUpdateSinkReq,
            SearchAPIsResp, SearchObservesResp, SearchSinksResp,
        },
        SearchDevicesItemResp,
    },
    Pagination,
};
use uuid::Uuid;

use crate::{AppResult, AppSuccess};

pub(crate) fn coap_routes() -> Router {
    Router::new()
        .route("/", post(create))
        .route("/:device_id", get(read))
        .route("/:device_id", put(update))
        .route("/:device_id/start", put(start))
        .route("/:device_id/stop", put(stop))
        .route("/:device_id", routing::delete(delete))
        .nest(
            "/:device_id",
            Router::new()
                .nest(
                    "/observe",
                    Router::new()
                        .route("/", post(create_observe))
                        .route("/", get(search_observes))
                        .route("/:observe_id", put(update_observe))
                        .route("/:observe_id", routing::delete(delete_observe)),
                )
                .nest(
                    "/api",
                    Router::new()
                        .route("/", post(create_api))
                        .route("/", get(search_apis))
                        .route("/:api_id", put(update_api))
                        .route("/:api_id", routing::delete(delete_api)),
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

async fn create(Json(req): Json<CreateUpdateCoapReq>) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER.create(None, req).await?;
    Ok(AppSuccess::empty())
}

async fn read(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<SearchDevicesItemResp>> {
    let data = GLOBAL_COAP_MANAGER.search(&device_id)?;
    Ok(AppSuccess::data(data))
}

async fn update(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateCoapReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER.update(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER.start(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER.stop(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER.delete(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_observe(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateObserveReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER
        .create_observe(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_observes(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchObservesResp>> {
    let data = GLOBAL_COAP_MANAGER
        .search_observes(device_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_observe(
    Path((device_id, api_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateObserveReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER
        .update_observe(device_id, api_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

pub async fn delete_observe(
    Path((device_id, observe_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER
        .delete_observe(device_id, observe_id)
        .await?;
    Ok(AppSuccess::empty())
}

async fn create_api(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateAPIReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER.create_api(device_id, None, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_apis(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchAPIsResp>> {
    let data = GLOBAL_COAP_MANAGER
        .search_apis(device_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_api(
    Path((device_id, api_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateAPIReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER
        .update_api(device_id, api_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

pub async fn delete_api(
    Path((device_id, api_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER.delete_api(device_id, api_id).await?;
    Ok(AppSuccess::empty())
}

pub async fn create_sink(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER
        .create_sink(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

pub async fn search_sinks(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchSinksResp>> {
    let data = GLOBAL_COAP_MANAGER
        .search_sinks(device_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

pub async fn update_sink(
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER
        .update_sink(device_id, sink_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

pub async fn delete_sink(
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_COAP_MANAGER.delete_sink(device_id, sink_id).await?;
    Ok(AppSuccess::empty())
}

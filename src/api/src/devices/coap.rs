use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use devices::coap::manager::GLOBAL_COAP_MANAGER;
use types::{
    devices::coap::{
        CreateUpdateAPIReq, CreateUpdateCoapReq, CreateUpdateSinkReq, SearchAPIsResp,
        SearchSinksResp,
    },
    Pagination,
};
use uuid::Uuid;

use crate::AppResp;

pub(crate) fn coap_routes() -> Router {
    Router::new()
        .route("/", post(create))
        .route("/:device_id", put(update))
        .route("/:device_id/start", put(start))
        .route("/:device_id/stop", put(stop))
        .route("/:device_id", routing::delete(delete))
        .nest(
            "/:device_id",
            Router::new()
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

async fn create(Json(req): Json<CreateUpdateCoapReq>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.create(None, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn update(Path(device_id): Path<Uuid>, Json(req): Json<CreateUpdateCoapReq>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.update(device_id, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn start(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.start(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn stop(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.stop(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.delete(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_api(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateAPIReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.create_api(device_id, None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search_apis(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResp<SearchAPIsResp> {
    match GLOBAL_COAP_MANAGER.search_apis(device_id, pagination).await {
        Ok(values) => AppResp::with_data(values),
        Err(e) => e.into(),
    }
}

async fn update_api(
    Path((device_id, api_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateAPIReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.update_api(device_id, api_id, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn delete_api(Path((device_id, api_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.delete_api(device_id, api_id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn create_sink(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.create_sink(device_id, None, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn search_sinks(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResp<SearchSinksResp> {
    match GLOBAL_COAP_MANAGER
        .search_sinks(device_id, pagination)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub async fn update_sink(
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER
        .update_sink(device_id, sink_id, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn delete_sink(Path((device_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.delete_sink(device_id, sink_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

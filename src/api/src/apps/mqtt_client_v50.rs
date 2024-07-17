use apps::mqtt_client_v311::manager::GLOBAL_MQTT_CLIENT_V311_MANAGER;
use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use types::apps::mqtt_client_v50::{
    CreateUpdateMqttClientReq, CreateUpdateSinkReq, CreateUpdateSourceReq, SearchSinksResp,
    SearchSourcesResp,
};
use uuid::Uuid;

use crate::{AppResp, Pagination};

pub fn mqtt_client_v50_routes() -> Router {
    Router::new().nest(
        "/mqtt_client_v50",
        Router::new()
            .route("/", post(create))
            .route("/:app_id", put(update))
            .route("/:app_id", routing::delete(delete))
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
            ),
    )
}

async fn create(Json(req): Json<CreateUpdateMqttClientReq>) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER.create(None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn update(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateMqttClientReq>,
) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER.update(app_id, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete(Path(app_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER.delete(app_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_source(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceReq>,
) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER
        .create_source(app_id, None, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search_sources(
    Path(app_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSourcesResp> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER
        .search_sources(app_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

async fn update_source(
    Path((app_id, source_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceReq>,
) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER
        .update_source(app_id, source_id, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete_source(Path((app_id, source_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER
        .delete_source(app_id, source_id)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_sink(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER
        .create_sink(app_id, None, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search_sinks(
    Path(app_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSinksResp> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER
        .search_sinks(app_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

async fn update_sink(
    Path((app_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER
        .update_sink(app_id, sink_id, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete_sink(Path((app_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_V311_MANAGER
        .delete_sink(app_id, sink_id)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

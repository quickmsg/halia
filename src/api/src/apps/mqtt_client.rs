use apps::mqtt_client::manager::GLOBAL_MQTT_CLIENT_MANAGER;
use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use types::{
    apps::mqtt_client::{
        CreateUpdateMqttClientReq, CreateUpdateSinkReq, CreateUpdateSourceReq, SearchSinksResp,
        SearchSourcesResp,
    },
    Pagination,
};
use uuid::Uuid;

use crate::{AppResult, AppSuccess};

pub fn mqtt_client_routes() -> Router {
    Router::new()
        .route("/", post(create))
        .route("/:app_id", put(update))
        .route("/:app_id/start", put(start))
        .route("/:app_id/stop", put(stop))
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
        )
}

async fn create(Json(req): Json<CreateUpdateMqttClientReq>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER.create(None, req).await?;
    Ok(AppSuccess::empty())
}

async fn update(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateMqttClientReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER.update(app_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start(Path(app_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER.start(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop(Path(app_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER.stop(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete(Path(app_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER.delete(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER
        .create_source(app_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchSourcesResp>> {
    let data = GLOBAL_MQTT_CLIENT_MANAGER
        .search_sources(app_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_source(
    Path((app_id, source_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER
        .update_source(app_id, source_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_source(Path((app_id, source_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER
        .delete_source(app_id, source_id)
        .await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER
        .create_sink(app_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchSinksResp>> {
    let data = GLOBAL_MQTT_CLIENT_MANAGER
        .search_sinks(app_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_sink(
    Path((app_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER
        .update_sink(app_id, sink_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(Path((app_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MQTT_CLIENT_MANAGER
        .delete_sink(app_id, sink_id)
        .await?;
    Ok(AppSuccess::empty())
}

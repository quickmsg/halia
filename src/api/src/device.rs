use axum::{
    extract::{Path, Query, State},
    routing::{delete, get, post, put},
    Json, Router,
};
use types::{
    devices::{QueryParams, SearchDevicesResp, Summary},
    Pagination, SearchSourcesOrSinksResp, Value,
};
use uuid::Uuid;

use crate::{AppResult, AppState, AppSuccess};

pub fn routes() -> Router<AppState> {
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

async fn get_devices_summary(State(state): State<AppState>) -> AppSuccess<Summary> {
    let summary = devices::get_summary(&state.devices).await;
    AppSuccess::data(summary)
}

async fn create_device(State(state): State<AppState>, body: String) -> AppResult<AppSuccess<()>> {
    devices::create_device(&state.pool, &state.devices, Uuid::new_v4(), body, true).await?;
    Ok(AppSuccess::empty())
}

async fn search_devices(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchDevicesResp> {
    AppSuccess::data(devices::search_devices(&state.devices, pagination, query_params).await)
}

async fn update_device(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    devices::update_device(&state.pool, &state.devices, device_id, body).await?;
    Ok(AppSuccess::empty())
}

async fn start_device(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    devices::start_device(&state.pool, &state.devices, device_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop_device(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    devices::stop_device(&state.pool, &state.devices, device_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete_device(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    devices::stop_device(&state.pool, &state.devices, device_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    devices::create_source(
        &state.pool,
        &state.devices,
        device_id,
        Uuid::new_v4(),
        body,
        true,
    )
    .await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sources =
        devices::search_sources(&state.devices, device_id, pagination, query_params).await?;
    Ok(AppSuccess::data(sources))
}

async fn update_source(
    State(state): State<AppState>,
    Path((device_id, source_id)): Path<(Uuid, Uuid)>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    devices::update_source(&state.pool, &state.devices, device_id, source_id, body).await?;
    Ok(AppSuccess::empty())
}

async fn write_source_value(
    State(state): State<AppState>,
    Path((device_id, source_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<Value>,
) -> AppResult<AppSuccess<()>> {
    devices::write_source_value(&state.devices, device_id, source_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_source(
    State(state): State<AppState>,
    Path((device_id, source_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    devices::delete_source(&state.pool, &state.devices, device_id, source_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    devices::create_sink(
        &state.pool,
        &state.devices,
        device_id,
        Uuid::new_v4(),
        body,
        true,
    )
    .await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sinks = devices::search_sinks(&state.devices, device_id, pagination, query).await?;
    Ok(AppSuccess::data(sinks))
}

async fn update_sink(
    State(state): State<AppState>,
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
    body: String,
) -> AppResult<AppSuccess<()>> {
    devices::update_sink(&state.pool, &state.devices, device_id, sink_id, body).await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(
    State(state): State<AppState>,
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    devices::delete_sink(&state.pool, &state.devices, device_id, sink_id).await?;
    Ok(AppSuccess::empty())
}

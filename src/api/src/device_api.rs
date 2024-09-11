use axum::{
    extract::{Path, Query, State},
    routing::{delete, get, post, put},
    Json, Router,
};
use types::{
    devices::{CreateUpdateDeviceReq, QueryParams, SearchDevicesResp, Summary},
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp, Value,
};
use uuid::Uuid;

use crate::{AppResult, AppState, AppSuccess};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/summary", get(get_devices_summary))
        // 查询事件
        // .route("/event", get(get_rule_info(state, query)))
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
    AppSuccess::data(devices::get_summary())
}

async fn create_device(
    State(state): State<AppState>,
    Json(req): Json<CreateUpdateDeviceReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_device(&state.storage, Uuid::new_v4(), req).await?;
    Ok(AppSuccess::empty())
}

async fn search_devices(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchDevicesResp>> {
    let resp =
        devices::search_devices(&state.storage, &state.devices, pagination, query_params).await?;
    Ok(AppSuccess::data(resp))
}

async fn update_device(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateDeviceReq>,
) -> AppResult<AppSuccess<()>> {
    devices::update_device(&state.storage, &state.devices, device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start_device(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    devices::start_device(&state.storage, &state.devices, device_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop_device(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    devices::stop_device(&state.storage, &state.devices, device_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete_device(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    devices::delete_device(&state.storage, &state.devices, device_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_source(
        &state.storage,
        &state.devices,
        device_id,
        Uuid::new_v4(),
        req,
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
    let sources = devices::search_sources(
        &state.storage,
        &state.devices,
        device_id,
        pagination,
        query_params,
    )
    .await?;
    Ok(AppSuccess::data(sources))
}

async fn update_source(
    State(state): State<AppState>,
    Path((device_id, source_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    devices::update_source(&state.storage, &state.devices, device_id, source_id, req).await?;
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
    devices::delete_source(&state.storage, &state.devices, device_id, source_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    State(state): State<AppState>,
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_sink(
        &state.storage,
        &state.devices,
        device_id,
        Uuid::new_v4(),
        req,
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
    devices::update_sink(&state.storage, &state.devices, device_id, sink_id, body).await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(
    State(state): State<AppState>,
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    devices::delete_sink(&state.storage, &state.devices, device_id, sink_id).await?;
    Ok(AppSuccess::empty())
}

use axum::{
    extract::{Path, Query},
    routing::{delete, get, post, put},
    Json, Router,
};
use types::{
    devices::{
        CreateUpdateDeviceReq, CreateUpdateSourceOrSinkReq, CreateUpdateSourceOrSinkTemplateReq,
        QueryParams, QuerySourceOrSinkTemplateParams, SearchDevicesResp,
        SearchSourcesOrSinkTemplatesResp, SearchSourcesOrSinksResp, Summary,
    },
    Pagination, QuerySourcesOrSinksParams, Value,
};

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
        .nest(
            "/device_template",
            Router::new()
                .route("/", post(create_device_template))
                .route("/", get(search_device_templates))
                .route("/:id", put(update_device_template))
                .route("/:id", delete(delete_device_template)),
        )
        .nest(
            "/source_template",
            Router::new()
                .route("/", post(create_source_template))
                .route("/", get(search_source_templates))
                .route("/:id", put(update_source_template))
                .route("/:id", delete(delete_source_template)),
        )
        .nest(
            "/sink_template",
            Router::new()
                .route("/", post(create_sink_template))
                .route("/", get(search_sink_templates))
                .route("/:id", put(update_source_template))
                .route("/:id", delete(delete_sink_template)),
        )
}

async fn get_devices_summary() -> AppSuccess<Summary> {
    AppSuccess::data(devices::get_summary())
}

async fn create_device(Json(req): Json<CreateUpdateDeviceReq>) -> AppResult<AppSuccess<()>> {
    devices::create_device(common::get_id(), req).await?;
    Ok(AppSuccess::empty())
}

async fn search_devices(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchDevicesResp>> {
    let resp = devices::search_devices(pagination, query_params).await?;
    Ok(AppSuccess::data(resp))
}

async fn update_device(
    Path(device_id): Path<String>,
    Json(req): Json<CreateUpdateDeviceReq>,
) -> AppResult<AppSuccess<()>> {
    devices::update_device(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start_device(Path(device_id): Path<String>) -> AppResult<AppSuccess<()>> {
    devices::start_device(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop_device(Path(device_id): Path<String>) -> AppResult<AppSuccess<()>> {
    devices::stop_device(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete_device(Path(device_id): Path<String>) -> AppResult<AppSuccess<()>> {
    devices::delete_device(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    Path(device_id): Path<String>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_source(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(
    Path(device_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QuerySourcesOrSinksParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sources = devices::search_sources(device_id, pagination, query_params).await?;
    Ok(AppSuccess::data(sources))
}

async fn update_source(
    Path((device_id, source_id)): Path<(String, String)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    devices::update_source(device_id, source_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn write_source_value(
    Path((device_id, source_id)): Path<(String, String)>,
    Json(req): Json<Value>,
) -> AppResult<AppSuccess<()>> {
    devices::write_source_value(device_id, source_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_source(
    Path((device_id, source_id)): Path<(String, String)>,
) -> AppResult<AppSuccess<()>> {
    devices::delete_source(device_id, source_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    Path(device_id): Path<String>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_sink(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    Path(device_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QuerySourcesOrSinksParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let sinks = devices::search_sinks(device_id, pagination, query).await?;
    Ok(AppSuccess::data(sinks))
}

async fn update_sink(
    Path((device_id, sink_id)): Path<(String, String)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    devices::update_sink(device_id, sink_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(
    Path((device_id, sink_id)): Path<(String, String)>,
) -> AppResult<AppSuccess<()>> {
    devices::delete_sink(device_id, sink_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_device_template(
    Json(req): Json<CreateUpdateSourceOrSinkTemplateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_source_template(req).await?;
    Ok(AppSuccess::empty())
}

async fn search_device_templates(
    Query(pagination): Query<Pagination>,
    Query(query): Query<QuerySourceOrSinkTemplateParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinkTemplatesResp>> {
    let data = devices::search_source_templates(pagination, query).await?;
    Ok(AppSuccess::data(data))
}

async fn update_device_template(
    Path(id): Path<String>,
    Json(req): Json<CreateUpdateSourceOrSinkTemplateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::update_source_template(id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_device_template(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    devices::delete_source_template(id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source_template(
    Json(req): Json<CreateUpdateSourceOrSinkTemplateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_source_template(req).await?;
    Ok(AppSuccess::empty())
}

async fn search_source_templates(
    Query(pagination): Query<Pagination>,
    Query(query): Query<QuerySourceOrSinkTemplateParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinkTemplatesResp>> {
    let data = devices::search_source_templates(pagination, query).await?;
    Ok(AppSuccess::data(data))
}

async fn update_source_template(
    Path(id): Path<String>,
    Json(req): Json<CreateUpdateSourceOrSinkTemplateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::update_source_template(id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_source_template(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    devices::delete_source_template(id).await?;
    Ok(AppSuccess::empty())
}

async fn create_sink_template(
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    // devices::create_sink_template(req).await?;
    // Ok(AppSuccess::empty())
    todo!()
}

async fn search_sink_templates() -> AppResult<AppSuccess<()>> {
    // devices::search_sink_templates().await?;
    // Ok(AppSuccess::empty())
    todo!()
}

async fn update_sink_template(
    Path(id): Path<String>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    // devices::update_sink_template(id, req).await?;
    // Ok(AppSuccess::empty())
    todo!()
}

async fn delete_sink_template(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    // devices::delete_sink_template(id).await?;
    // Ok(AppSuccess::empty())
    todo!()
}

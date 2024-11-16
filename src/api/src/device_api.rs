use axum::{
    extract::{Path, Query},
    routing::{delete, get, post, put},
    Json, Router,
};
use types::{devices::Summary, Pagination, QuerySourcesOrSinksParams, Value};

use crate::AppResult;

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_devices_summary))
        .route("/", post(create_device))
        .route("/list", get(list_devices))
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
                .route("/:id", put(update_sink_template))
                .route("/:id", delete(delete_sink_template)),
        )
        .nest(
            "/device_template",
            Router::new()
                .route("/", post(create_device_template))
                .route("/", get(search_device_templates))
                .route("/:id", get(read_device_template))
                .route("/:id", put(update_device_template))
                .route("/:id", delete(delete_device_template))
                .nest(
                    "/:device_template_id",
                    Router::new()
                        .nest(
                            "/source",
                            Router::new()
                                .route("/", post(create_device_template_source))
                                .route("/", get(search_device_template_sources))
                                .route("/:source_id", put(update_device_template_source))
                                .route("/:source_id", delete(delete_device_template_source)),
                        )
                        .nest(
                            "/sink",
                            Router::new()
                                .route("/", post(create_device_template_sink))
                                .route("/", get(search_device_template_sinks))
                                .route("/:sink_id", put(update_device_template_sink))
                                .route("/:sink_id", delete(delete_device_template_sink)),
                        ),
                ),
        )
}

async fn get_devices_summary() -> AppResult<Json<Summary>> {
    Ok(Json(devices::get_summary()))
}

async fn create_device(Json(req): Json<types::devices::device::CreateReq>) -> AppResult<()> {
    devices::create_device(common::get_id(), req).await?;
    Ok(())
}

async fn list_devices(
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device::QueryParams>,
) -> AppResult<Json<types::devices::ListDevicesResp>> {
    let resp = devices::list_devices(pagination, query).await?;
    Ok(Json(resp))
}

async fn update_device(
    Path(device_id): Path<String>,
    Json(req): Json<types::devices::device::UpdateReq>,
) -> AppResult<()> {
    devices::update_device(device_id, req).await?;
    Ok(())
}

async fn start_device(Path(device_id): Path<String>) -> AppResult<()> {
    devices::start_device(device_id).await?;
    Ok(())
}

async fn stop_device(Path(device_id): Path<String>) -> AppResult<()> {
    devices::stop_device(device_id).await?;
    Ok(())
}

async fn delete_device(Path(device_id): Path<String>) -> AppResult<()> {
    devices::delete_device(device_id).await?;
    Ok(())
}

async fn create_source(
    Path(device_id): Path<String>,
    Json(req): Json<types::devices::device::source_sink::CreateUpdateReq>,
) -> AppResult<()> {
    devices::device_create_source(device_id, req).await?;
    Ok(())
}

async fn search_sources(
    Path(device_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QuerySourcesOrSinksParams>,
) -> AppResult<Json<types::devices::device::source_sink::SearchResp>> {
    let resp = devices::search_sources(device_id, pagination, query_params).await?;
    Ok(Json(resp))
}

async fn update_source(
    Path((device_id, source_id)): Path<(String, String)>,
    Json(req): Json<types::devices::device::source_sink::CreateUpdateReq>,
) -> AppResult<()> {
    devices::update_source(device_id, source_id, req).await?;
    Ok(())
}

async fn write_source_value(
    Path((device_id, source_id)): Path<(String, String)>,
    Json(req): Json<Value>,
) -> AppResult<()> {
    devices::write_source_value(device_id, source_id, req).await?;
    Ok(())
}

async fn delete_source(Path((device_id, source_id)): Path<(String, String)>) -> AppResult<()> {
    devices::device_delete_source(device_id, source_id).await?;
    Ok(())
}

async fn create_sink(
    Path(device_id): Path<String>,
    Json(req): Json<types::devices::device::source_sink::CreateUpdateReq>,
) -> AppResult<()> {
    devices::device_create_sink(device_id, req).await?;
    Ok(())
}

async fn search_sinks(
    Path(device_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QuerySourcesOrSinksParams>,
) -> AppResult<Json<types::devices::device::source_sink::SearchResp>> {
    let resp = devices::search_sinks(device_id, pagination, query).await?;
    Ok(Json(resp))
}

async fn update_sink(
    Path((device_id, sink_id)): Path<(String, String)>,
    Json(req): Json<types::devices::device::source_sink::CreateUpdateReq>,
) -> AppResult<()> {
    devices::update_sink(device_id, sink_id, req).await?;
    Ok(())
}

async fn delete_sink(Path((device_id, sink_id)): Path<(String, String)>) -> AppResult<()> {
    devices::device_delete_sink(device_id, sink_id).await?;
    Ok(())
}

async fn create_source_template(
    Json(req): Json<types::devices::source_sink_template::CreateReq>,
) -> AppResult<()> {
    devices::source_sink_template::create_source_template(req).await?;
    Ok(())
}

async fn search_source_templates(
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::source_sink_template::QueryParams>,
) -> AppResult<Json<types::devices::source_sink_template::SearchResp>> {
    let resp = devices::source_sink_template::search_source_templates(pagination, query).await?;
    Ok(Json(resp))
}

async fn update_source_template(
    Path(id): Path<String>,
    Json(req): Json<types::devices::source_sink_template::UpdateReq>,
) -> AppResult<()> {
    devices::source_sink_template::update_source_template(id, req).await?;
    Ok(())
}

async fn delete_source_template(Path(id): Path<String>) -> AppResult<()> {
    devices::source_sink_template::delete_source_template(id).await?;
    Ok(())
}

async fn create_sink_template(
    Json(req): Json<types::devices::source_sink_template::CreateReq>,
) -> AppResult<()> {
    devices::source_sink_template::create_sink_template(req).await?;
    Ok(())
}

async fn search_sink_templates(
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::source_sink_template::QueryParams>,
) -> AppResult<Json<types::devices::source_sink_template::SearchResp>> {
    let resp = devices::source_sink_template::search_sink_templates(pagination, query).await?;
    Ok(Json(resp))
}

async fn update_sink_template(
    Path(id): Path<String>,
    Json(req): Json<types::devices::source_sink_template::UpdateReq>,
) -> AppResult<()> {
    devices::source_sink_template::update_sink_template(id, req).await?;
    Ok(())
}

async fn delete_sink_template(Path(id): Path<String>) -> AppResult<()> {
    devices::source_sink_template::delete_sink_template(id).await?;
    Ok(())
}

async fn create_device_template(
    Json(req): Json<types::devices::device_template::CreateReq>,
) -> AppResult<()> {
    devices::device_template::create_device_template(req).await?;
    Ok(())
}

async fn search_device_templates(
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device_template::QueryParams>,
) -> AppResult<Json<types::devices::device_template::SearchResp>> {
    let resp = devices::device_template::search_device_templates(pagination, query).await?;
    Ok(Json(resp))
}

async fn read_device_template(
    Path(id): Path<String>,
) -> AppResult<Json<types::devices::device_template::ReadResp>> {
    let resp = devices::device_template::read_device_template(id).await?;
    Ok(Json(resp))
}

async fn update_device_template(
    Path(id): Path<String>,
    Json(req): Json<types::devices::device_template::UpdateReq>,
) -> AppResult<()> {
    devices::device_template::update_device_template(id, req).await?;
    Ok(())
}

async fn delete_device_template(Path(id): Path<String>) -> AppResult<()> {
    devices::device_template::delete_device_template(id).await?;
    Ok(())
}

async fn create_device_template_source(
    Path(device_template_id): Path<String>,
    Json(req): Json<types::devices::device_template::source_sink::CreateUpdateReq>,
) -> AppResult<()> {
    devices::device_template::create_source(device_template_id, req).await?;
    Ok(())
}

async fn search_device_template_sources(
    Path(device_template_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device_template::source_sink::QueryParams>,
) -> AppResult<Json<types::devices::device_template::source_sink::SearchResp>> {
    let resp =
        devices::device_template::search_sources(device_template_id, pagination, query).await?;
    Ok(Json(resp))
}

async fn update_device_template_source(
    Path((device_template_id, source_id)): Path<(String, String)>,
    Json(req): Json<types::devices::device_template::source_sink::CreateUpdateReq>,
) -> AppResult<()> {
    devices::device_template::update_source(device_template_id, source_id, req).await?;
    Ok(())
}

async fn delete_device_template_source(
    Path((device_template_id, source_id)): Path<(String, String)>,
) -> AppResult<()> {
    devices::device_template::delete_source(device_template_id, source_id).await?;
    Ok(())
}

async fn create_device_template_sink(
    Path(device_template_id): Path<String>,
    Json(req): Json<types::devices::device_template::source_sink::CreateUpdateReq>,
) -> AppResult<()> {
    devices::device_template::create_sink(device_template_id, req).await?;
    Ok(())
}

async fn search_device_template_sinks(
    Path(device_template_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device_template::source_sink::QueryParams>,
) -> AppResult<Json<types::devices::device_template::source_sink::SearchResp>> {
    let resp =
        devices::device_template::search_sinks(device_template_id, pagination, query).await?;
    Ok(Json(resp))
}

async fn update_device_template_sink(
    Path((device_template_id, sink_id)): Path<(String, String)>,
    Json(req): Json<types::devices::device_template::source_sink::CreateUpdateReq>,
) -> AppResult<()> {
    devices::device_template::update_sink(device_template_id, sink_id, req).await?;
    Ok(())
}

async fn delete_device_template_sink(
    Path((device_template_id, sink_id)): Path<(String, String)>,
) -> AppResult<()> {
    devices::device_template::delete_sink(device_template_id, sink_id).await?;
    Ok(())
}

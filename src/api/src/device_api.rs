use axum::{
    extract::{Path, Query},
    routing::{delete, get, post, put},
    Json, Router,
};
use types::{devices::Summary, Pagination, QuerySourcesOrSinksParams, Value};

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
        .nest(
            "/device_template",
            Router::new()
                .route("/", post(create_device_template))
                .route("/", get(search_device_templates))
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

async fn get_devices_summary() -> AppSuccess<Summary> {
    AppSuccess::data(devices::get_summary())
}

async fn create_device(
    Json(req): Json<types::devices::device::CreateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_device(common::get_id(), req).await?;
    Ok(AppSuccess::empty())
}

async fn search_devices(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<types::devices::device::QueryParams>,
) -> AppResult<AppSuccess<types::devices::device::SearchResp>> {
    let resp = devices::search_devices(pagination, query_params).await?;
    Ok(AppSuccess::data(resp))
}

async fn update_device(
    Path(device_id): Path<String>,
    Json(req): Json<types::devices::device::UpdateReq>,
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
    Json(req): Json<types::devices::device::source_sink::CreateUpdateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_source(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(
    Path(device_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QuerySourcesOrSinksParams>,
) -> AppResult<AppSuccess<types::devices::device::source_sink::SearchResp>> {
    let sources = devices::search_sources(device_id, pagination, query_params).await?;
    Ok(AppSuccess::data(sources))
}

async fn update_source(
    Path((device_id, source_id)): Path<(String, String)>,
    Json(req): Json<types::devices::device::source_sink::CreateUpdateReq>,
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
    Json(req): Json<types::devices::device::source_sink::CreateUpdateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::create_sink(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    Path(device_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QuerySourcesOrSinksParams>,
) -> AppResult<AppSuccess<types::devices::device::source_sink::SearchResp>> {
    let sinks = devices::search_sinks(device_id, pagination, query).await?;
    Ok(AppSuccess::data(sinks))
}

async fn update_sink(
    Path((device_id, sink_id)): Path<(String, String)>,
    Json(req): Json<types::devices::device::source_sink::CreateUpdateReq>,
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

async fn create_source_template(
    Json(req): Json<types::devices::source_sink_template::CreateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::source_sink_template::create(req).await?;
    Ok(AppSuccess::empty())
}

async fn search_source_templates(
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::source_sink_template::QueryParams>,
) -> AppResult<AppSuccess<types::devices::source_sink_template::SearchResp>> {
    let data = devices::source_sink_template::search_source_templates(pagination, query).await?;
    Ok(AppSuccess::data(data))
}

async fn update_source_template(
    Path(id): Path<String>,
    Json(req): Json<types::devices::source_sink_template::UpdateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::source_sink_template::update(id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_source_template(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    devices::source_sink_template::delete(id).await?;
    Ok(AppSuccess::empty())
}

async fn create_sink_template(
    Json(req): Json<types::devices::source_sink_template::CreateReq>,
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
    Json(req): Json<types::devices::source_sink_template::UpdateReq>,
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

async fn create_device_template(
    Json(req): Json<types::devices::device_template::CreateReq>,
) -> AppResult<AppSuccess<()>> {
    devices::device_template::create(req).await?;
    Ok(AppSuccess::empty())
}

async fn search_device_templates(
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device_template::QueryParams>,
) -> AppResult<AppSuccess<types::devices::device_template::SearchResp>> {
    let data = devices::device_template::search(pagination, query).await?;
    Ok(AppSuccess::data(data))
}

async fn update_device_template(
    Path(id): Path<String>,
    Json(req): Json<types::devices::device_template::UpdateReq>,
) -> AppResult<AppSuccess<()>> {
    todo!()
    // devices::update_source_template(id, req).await?;
    // Ok(AppSuccess::empty())
}

async fn delete_device_template(Path(id): Path<String>) -> AppResult<AppSuccess<()>> {
    todo!()
    // devices::delete_source_template(id).await?;
    // Ok(AppSuccess::empty())
}

async fn create_device_template_source(
    Path(device_template_id): Path<String>,
    Json(req): Json<types::devices::device_template::source_sink::CreateUpdateReq>,
) -> AppResult<AppSuccess<()>> {
    todo!()
    // devices::create_device_template_source(device_template_id, req).await?;
    // Ok(AppSuccess::empty())
}

async fn search_device_template_sources(
    Path(device_template_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device_template::source_sink::QueryParams>,
) -> AppResult<AppSuccess<types::devices::device_template::source_sink::SearchResp>> {
    todo!()
    // let data = devices::search_device_template_sources(device_template_id, pagination, query).await?;
    // Ok(AppSuccess::data(data))
}

async fn update_device_template_source(
    Path((device_template_id, source_id)): Path<(String, String)>,
    Json(req): Json<types::devices::device_template::source_sink::CreateUpdateReq>,
) -> AppResult<AppSuccess<()>> {
    todo!()
    // devices::update_device_template_source(device_template_id, source_id, req).await?;
    // Ok(AppSuccess::empty())
}

async fn delete_device_template_source(
    Path((device_template_id, source_id)): Path<(String, String)>,
) -> AppResult<AppSuccess<()>> {
    todo!()
    // devices::delete_device_template_source(device_template_id, source_id).await?;
    // Ok(AppSuccess::empty())
}

async fn create_device_template_sink(
    Path(device_template_id): Path<String>,
    Json(req): Json<types::devices::device_template::source_sink::CreateUpdateReq>,
) -> AppResult<AppSuccess<()>> {
    todo!()
    // devices::create_device_template_sink(device_template_id, req).await?;
    // Ok(AppSuccess::empty())
}

async fn search_device_template_sinks(
    Path(device_template_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device_template::source_sink::QueryParams>,
) -> AppResult<AppSuccess<types::devices::device_template::source_sink::SearchResp>> {
    todo!()
    // let data = devices::search_device_template_sinks(device_template_id, pagination, query).await?;
    // Ok(AppSuccess::data(data))
}

async fn update_device_template_sink(
    Path((device_template_id, sink_id)): Path<(String, String)>,
    Json(req): Json<types::devices::device_template::source_sink::CreateUpdateReq>,
) -> AppResult<AppSuccess<()>> {
    todo!()
    // devices::update_device_template_sink(device_template_id, sink_id, req).await?;
    // Ok(AppSuccess::empty())
}

async fn delete_device_template_sink(
    Path((device_template_id, sink_id)): Path<(String, String)>,
) -> AppResult<AppSuccess<()>> {
    todo!()
    // devices::delete_device_template_sink(device_template_id, sink_id).await?;
    // Ok(AppSuccess::empty())
}

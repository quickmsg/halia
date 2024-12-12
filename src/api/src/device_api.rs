use axum::{
    extract::{Path, Query},
    routing::{delete, get, post, put},
    Json, Router,
};
use types::{
    devices::{
        DeviceSourceGroupListResp, DeviceSourceGroupQueryParams, DeviceSourceGroupReadResp,
        DeviceSourceGroupUpdateReq, ListSourcesSinksResp, ReadSourceSinkResp,
        SourceSinkCreateUpdateReq, SourceSinkQueryParams,
    },
    Pagination, Summary, Value,
};

use crate::AppResult;

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_devices_summary))
        .route("/", post(create_device))
        .route("/list", get(list_devices))
        .route("/:device_id", get(read_device))
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
                        .route("/list", get(list_sources))
                        .route("/:source_id", get(read_source))
                        .route("/:source_id", put(update_source))
                        .route("/:source_id/value", put(write_source_value))
                        .route("/:source_id", delete(delete_source)),
                )
                .nest(
                    "/source_group",
                    Router::new()
                        .route("/", post(device_create_source_group))
                        .route("/list", get(device_list_source_groups))
                        .route("/:source_group_id", get(device_read_source_group))
                        .route("/:source_group_id", put(device_update_source_group))
                        .route("/:source_group_id", delete(device_delete_source_group)),
                )
                .nest(
                    "/sink",
                    Router::new()
                        .route("/", post(create_sink))
                        .route("/list", get(list_sinks))
                        .route("/:sink_id", get(read_sink))
                        .route("/:sink_id", put(update_sink))
                        .route("/:sink_id", delete(delete_sink)),
                ),
        )
        .nest(
            "/source_group",
            Router::new()
                .route("/", post(create_source_group))
                .route("/list", get(list_source_groups))
                .route("/:id", get(read_source_group))
                .route("/:id", put(update_source_group))
                .route("/:id", delete(delete_source_group))
                .nest(
                    "/:source_group_id/source",
                    Router::new()
                        .route("/", post(create_source_group_source))
                        .route("/list", get(list_source_group_sources))
                        .route("/:source_id", get(read_source_group_source))
                        .route("/:source_id", put(update_source_group_source))
                        .route("/:source_id", delete(delete_source_group_source)),
                ),
        )
        .nest(
            "/device_template",
            Router::new()
                .route("/", post(create_device_template))
                .route("/list", get(list_device_templates))
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
                                .route("/list", get(list_device_template_sources))
                                .route("/:source_id", get(read_device_template_source))
                                .route("/:source_id", put(update_device_template_source))
                                .route("/:source_id", delete(delete_device_template_source)),
                        )
                        .nest(
                            "/sink",
                            Router::new()
                                .route("/", post(create_device_template_sink))
                                .route("/list", get(list_device_template_sinks))
                                .route("/:sink_id", get(read_device_template_sink))
                                .route("/:sink_id", put(update_device_template_sink))
                                .route("/:sink_id", delete(delete_device_template_sink)),
                        ),
                ),
        )
}

async fn get_devices_summary() -> AppResult<Json<Summary>> {
    Ok(Json(devices::get_summary().await?))
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

async fn read_device(
    Path(device_id): Path<String>,
) -> AppResult<Json<types::devices::ReadDeviceResp>> {
    let resp = devices::read_device(device_id).await?;
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
    Json(req): Json<SourceSinkCreateUpdateReq>,
) -> AppResult<()> {
    devices::device_create_source(device_id, req).await?;
    Ok(())
}

async fn list_sources(
    Path(device_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<SourceSinkQueryParams>,
) -> AppResult<Json<ListSourcesSinksResp>> {
    let resp = devices::list_sources(device_id, pagination, query).await?;
    Ok(Json(resp))
}

async fn read_source(
    Path((device_id, source_id)): Path<(String, String)>,
) -> AppResult<Json<ReadSourceSinkResp>> {
    let resp = devices::read_source(device_id, source_id).await?;
    Ok(Json(resp))
}

async fn update_source(
    Path((device_id, source_id)): Path<(String, String)>,
    Json(req): Json<SourceSinkCreateUpdateReq>,
) -> AppResult<()> {
    devices::device_update_source(device_id, source_id, req).await?;
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

async fn device_create_source_group(
    Path(device_id): Path<String>,
    Json(req): Json<types::devices::DeviceSourceGroupCreateReq>,
) -> AppResult<()> {
    devices::create_source_group(device_id, req).await?;
    Ok(())
}

async fn device_list_source_groups(
    Path(device_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<DeviceSourceGroupQueryParams>,
) -> AppResult<Json<DeviceSourceGroupListResp>> {
    let resp = devices::list_source_groups(device_id, pagination, query).await?;
    Ok(Json(resp))
}

async fn device_read_source_group(
    Path((device_id, source_id)): Path<(String, String)>,
) -> AppResult<Json<DeviceSourceGroupReadResp>> {
    let resp = devices::read_source_group(device_id, source_id).await?;
    Ok(Json(resp))
}

async fn device_update_source_group(
    Path((device_id, source_id)): Path<(String, String)>,
    Json(req): Json<DeviceSourceGroupUpdateReq>,
) -> AppResult<()> {
    devices::update_source_group(device_id, source_id, req).await?;
    Ok(())
}

async fn device_delete_source_group(
    Path((device_id, source_id)): Path<(String, String)>,
) -> AppResult<()> {
    devices::delete_source_group(device_id, source_id).await?;
    Ok(())
}

async fn create_sink(
    Path(device_id): Path<String>,
    Json(req): Json<SourceSinkCreateUpdateReq>,
) -> AppResult<()> {
    devices::device_create_sink(device_id, req).await?;
    Ok(())
}

async fn list_sinks(
    Path(device_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<SourceSinkQueryParams>,
) -> AppResult<Json<ListSourcesSinksResp>> {
    let resp = devices::list_sinks(device_id, pagination, query).await?;
    Ok(Json(resp))
}

async fn read_sink(
    Path((device_id, sink_id)): Path<(String, String)>,
) -> AppResult<Json<ReadSourceSinkResp>> {
    let resp = devices::read_sink(device_id, sink_id).await?;
    Ok(Json(resp))
}

async fn update_sink(
    Path((device_id, sink_id)): Path<(String, String)>,
    Json(req): Json<SourceSinkCreateUpdateReq>,
) -> AppResult<()> {
    devices::device_update_sink(device_id, sink_id, req).await?;
    Ok(())
}

async fn delete_sink(Path((device_id, sink_id)): Path<(String, String)>) -> AppResult<()> {
    devices::device_delete_sink(device_id, sink_id).await?;
    Ok(())
}

async fn create_source_group(
    Json(req): Json<types::devices::source_group::CreateReq>,
) -> AppResult<()> {
    devices::source_group::create_source_group(req).await?;
    Ok(())
}

async fn list_source_groups(
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::source_group::QueryParams>,
) -> AppResult<Json<types::devices::source_group::ListResp>> {
    let resp = devices::source_group::list_source_groups(pagination, query).await?;
    Ok(Json(resp))
}

async fn read_source_group(
    Path(id): Path<String>,
) -> AppResult<Json<types::devices::source_group::ReadResp>> {
    let resp = devices::source_group::read_source_group(id).await?;
    Ok(Json(resp))
}

async fn update_source_group(
    Path(id): Path<String>,
    Json(req): Json<types::devices::source_group::UpdateReq>,
) -> AppResult<()> {
    devices::source_group::update_source_group(id, req).await?;
    Ok(())
}

async fn delete_source_group(Path(id): Path<String>) -> AppResult<()> {
    devices::source_group::delete_source_group(id).await?;
    Ok(())
}

async fn create_source_group_source(
    Path(source_group_id): Path<String>,
    Json(req): Json<types::devices::source_group::CreateUpdateSourceReq>,
) -> AppResult<()> {
    devices::source_group::create_source(source_group_id, req).await?;
    Ok(())
}

async fn list_source_group_sources(
    Path(source_group_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::source_group::SourceQueryParams>,
) -> AppResult<Json<types::devices::source_group::ListSourcesResp>> {
    let resp = devices::source_group::list_sources(source_group_id, pagination, query).await?;
    Ok(Json(resp))
}

async fn read_source_group_source(
    Path((source_group_id, source_id)): Path<(String, String)>,
) -> AppResult<Json<types::devices::source_group::ReadSourceResp>> {
    let resp = devices::source_group::read_source(source_group_id, source_id).await?;
    Ok(Json(resp))
}

async fn update_source_group_source(
    Path((source_group_id, source_id)): Path<(String, String)>,
    Json(req): Json<types::devices::source_group::CreateUpdateSourceReq>,
) -> AppResult<()> {
    devices::source_group::update_source(source_group_id, source_id, req).await?;
    Ok(())
}

async fn delete_source_group_source(
    Path((source_group_id, source_id)): Path<(String, String)>,
) -> AppResult<()> {
    devices::source_group::delete_source(source_group_id, source_id).await?;
    Ok(())
}

async fn create_device_template(
    Json(req): Json<types::devices::device_template::CreateReq>,
) -> AppResult<()> {
    devices::device_template::create_device_template(req).await?;
    Ok(())
}

async fn list_device_templates(
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device_template::QueryParams>,
) -> AppResult<Json<types::devices::device_template::ListResp>> {
    let resp = devices::device_template::list_device_templates(pagination, query).await?;
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
    Json(req): Json<SourceSinkCreateUpdateReq>,
) -> AppResult<()> {
    devices::device_template::create_source(device_template_id, req).await?;
    Ok(())
}

async fn list_device_template_sources(
    Path(device_template_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device_template::source_sink::QueryParams>,
) -> AppResult<Json<types::devices::device_template::source_sink::ListResp>> {
    Ok(Json(
        devices::device_template::list_sources(device_template_id, pagination, query).await?,
    ))
}

async fn read_device_template_source(
    Path((device_template_id, source_id)): Path<(String, String)>,
) -> AppResult<Json<types::devices::device_template::source_sink::ReadResp>> {
    Ok(Json(
        devices::device_template::read_source(device_template_id, source_id).await?,
    ))
}

async fn update_device_template_source(
    Path((device_template_id, source_id)): Path<(String, String)>,
    Json(req): Json<SourceSinkCreateUpdateReq>,
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
    Json(req): Json<SourceSinkCreateUpdateReq>,
) -> AppResult<()> {
    devices::device_template::create_sink(device_template_id, req).await?;
    Ok(())
}

async fn list_device_template_sinks(
    Path(device_template_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<types::devices::device_template::source_sink::QueryParams>,
) -> AppResult<Json<types::devices::device_template::source_sink::ListResp>> {
    Ok(Json(
        devices::device_template::list_sinks(device_template_id, pagination, query).await?,
    ))
}

async fn read_device_template_sink(
    Path((device_template_id, sink_id)): Path<(String, String)>,
) -> AppResult<Json<types::devices::device_template::source_sink::ReadResp>> {
    Ok(Json(
        devices::device_template::read_sink(device_template_id, sink_id).await?,
    ))
}

async fn update_device_template_sink(
    Path((device_template_id, sink_id)): Path<(String, String)>,
    Json(req): Json<SourceSinkCreateUpdateReq>,
) -> AppResult<()> {
    Ok(devices::device_template::update_sink(device_template_id, sink_id, req).await?)
}

async fn delete_device_template_sink(
    Path((device_template_id, sink_id)): Path<(String, String)>,
) -> AppResult<()> {
    devices::device_template::delete_sink(device_template_id, sink_id).await?;
    Ok(())
}

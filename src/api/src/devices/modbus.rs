use axum::{
    extract::{Json, Path, Query},
    routing::{self, get, post, put},
    Router,
};
use devices::modbus::manager::GLOBAL_MODBUS_MANAGER;
use types::{
    devices::{
        modbus::{
            CreateUpdateModbusReq, CreateUpdatePointReq, CreateUpdateSinkReq, PointsQueryParams,
            SearchPointsResp, SearchSinksResp, SinksQueryParams,
        },
        SearchDevicesItemResp,
    },
    Pagination, Value,
};
use uuid::Uuid;

use crate::{AppResult, AppSuccess};

pub(crate) fn modbus_routes() -> Router {
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
                    "/point",
                    Router::new()
                        .route("/", post(create_point))
                        .route("/", get(search_points))
                        .route("/:point_id", put(update_point))
                        .route("/:point_id/value", put(write_point_value))
                        .route("/:point_id", routing::delete(delete_point)),
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

async fn create(Json(req): Json<CreateUpdateModbusReq>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER.create(None, req).await?;
    Ok(AppSuccess::empty())
}

async fn read(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<SearchDevicesItemResp>> {
    let data = GLOBAL_MODBUS_MANAGER.search(&device_id).await?;
    Ok(AppSuccess::data(data))
}

async fn update(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateModbusReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER.update(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER.start(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER.stop(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER.delete(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_point(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdatePointReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER
        .create_point(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_points(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<PointsQueryParams>,
) -> AppResult<AppSuccess<SearchPointsResp>> {
    let data = GLOBAL_MODBUS_MANAGER
        .search_points(device_id, pagination, query_params)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_point(
    Path((device_id, point_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdatePointReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER
        .update_point(device_id, point_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn write_point_value(
    Path((device_id, point_id)): Path<(Uuid, Uuid)>,
    Json(value): Json<Value>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER
        .write_point_value(device_id, point_id, value)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_point(
    Path((device_id, point_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER
        .delete_point(device_id, point_id)
        .await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER
        .create_sink(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<SinksQueryParams>,
) -> AppResult<AppSuccess<SearchSinksResp>> {
    let data = GLOBAL_MODBUS_MANAGER
        .search_sinks(device_id, pagination, query_params)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_sink(
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER
        .update_sink(device_id, sink_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(Path((device_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_MODBUS_MANAGER
        .delete_sink(device_id, sink_id)
        .await?;
    Ok(AppSuccess::empty())
}

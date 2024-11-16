use axum::{
    extract::{Path, Query},
    routing::{delete, get, post, put},
    Json, Router,
};
use types::{
    databoard::{
        CreateUpdateDataReq, CreateUpdateDataboardReq, QueryDatasParams, QueryParams,
        SearchDatasResp, Summary,
    },
    Pagination,
};

use crate::AppResult;

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_databoards_summary))
        .route("/", post(create_databoard))
        .route("/list", get(list_databoards))
        .route("/:databoard_id", put(update_databoard))
        .route("/:databoard_id/start", put(start_databoard))
        .route("/:databoard_id/stop", put(stop_databoard))
        .route("/:databoard_id", delete(delete_databoard))
        .nest(
            "/:databoard_id/data",
            Router::new()
                .route("/", post(create_data))
                .route("/", get(search_datas))
                .route("/:data_id", put(update_data))
                .route("/:data_id", delete(delete_data)),
        )
}

async fn get_databoards_summary() -> AppResult<Json<Summary>> {
    Ok(Json(databoard::get_summary()))
}

async fn create_databoard(Json(req): Json<CreateUpdateDataboardReq>) -> AppResult<()> {
    databoard::create_databoard(req).await?;
    Ok(())
}

async fn list_databoards(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<Json<String>> {
    todo!()
    // let resp = databoard::search_databoards(pagination, query_params).await?;
    // Ok(Json(resp))
}

async fn update_databoard(
    Path(databoard_id): Path<String>,
    Json(req): Json<CreateUpdateDataboardReq>,
) -> AppResult<()> {
    databoard::update_databoard(databoard_id, req).await?;
    Ok(())
}

async fn start_databoard(Path(databoard_id): Path<String>) -> AppResult<()> {
    databoard::start_databoard(databoard_id).await?;
    Ok(())
}

async fn stop_databoard(Path(databoard_id): Path<String>) -> AppResult<()> {
    databoard::stop_databoard(databoard_id).await?;
    Ok(())
}

async fn delete_databoard(Path(databoard_id): Path<String>) -> AppResult<()> {
    databoard::delete_databoard(databoard_id).await?;
    Ok(())
}

async fn create_data(
    Path(databoard_id): Path<String>,
    Json(req): Json<CreateUpdateDataReq>,
) -> AppResult<()> {
    databoard::create_data(databoard_id, req).await?;
    Ok(())
}

async fn search_datas(
    Path(databoard_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryDatasParams>,
) -> AppResult<Json<SearchDatasResp>> {
    let resp = databoard::search_datas(databoard_id, pagination, query_params).await?;
    Ok(Json(resp))
}

async fn update_data(
    Path((databoard_id, databoard_data_id)): Path<(String, String)>,
    Json(req): Json<CreateUpdateDataReq>,
) -> AppResult<()> {
    databoard::update_data(databoard_id, databoard_data_id, req).await?;
    Ok(())
}

async fn delete_data(
    Path((databoard_id, databoard_data_id)): Path<(String, String)>,
) -> AppResult<()> {
    databoard::delete_data(databoard_id, databoard_data_id).await?;
    Ok(())
}

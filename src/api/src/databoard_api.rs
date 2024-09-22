use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use types::{
    databoard::{
        CreateUpdateDataReq, CreateUpdateDataboardReq, QueryParams, QueryRuleInfo,
        SearchDataboardsResp, SearchDatasResp, SearchRuleInfo, Summary,
    },
    Pagination,
};

use crate::{AppResult, AppSuccess};

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_databoards_summary))
        .route("/rule", get(get_rule_info))
        .route("/", post(create_databoard))
        .route("/", get(search_databoards))
        .route("/:databoard_id", put(update_databoard))
        .route("/:databoard_id", routing::delete(delete_databoard))
        .nest(
            "/:databoard_id/data",
            Router::new()
                .route("/", post(create_data))
                .route("/", get(search_datas))
                .route("/:data_id", put(update_data))
                .route("/:data_id", routing::delete(delete_data)),
        )
}

async fn get_databoards_summary() -> AppSuccess<Summary> {
    AppSuccess::data(databoard::get_summary())
}

async fn get_rule_info(
    Query(query): Query<QueryRuleInfo>,
) -> AppResult<AppSuccess<SearchRuleInfo>> {
    let rule_info = databoard::get_rule_info(query).await?;
    Ok(AppSuccess::data(rule_info))
}

async fn create_databoard(Json(req): Json<CreateUpdateDataboardReq>) -> AppResult<AppSuccess<()>> {
    databoard::create_databoard(req).await?;
    Ok(AppSuccess::empty())
}

async fn search_databoards(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchDataboardsResp>> {
    let resp = databoard::search_databoards(pagination, query_params).await?;
    Ok(AppSuccess::data(resp))
}

async fn update_databoard(
    Path(databoard_id): Path<String>,
    Json(req): Json<CreateUpdateDataboardReq>,
) -> AppResult<AppSuccess<()>> {
    databoard::update_databoard(databoard_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_databoard(Path(databoard_id): Path<String>) -> AppResult<AppSuccess<()>> {
    databoard::delete_databoard(databoard_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_data(
    Path(databoard_id): Path<String>,
    Json(req): Json<CreateUpdateDataReq>,
) -> AppResult<AppSuccess<()>> {
    databoard::create_data(databoard_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_datas(
    Path(databoard_id): Path<String>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchDatasResp>> {
    let data = databoard::search_datas(databoard_id, pagination, query).await?;
    Ok(AppSuccess::data(data))
}

async fn update_data(
    Path((databoard_id, databoard_data_id)): Path<(String, String)>,
    Json(req): Json<CreateUpdateDataReq>,
) -> AppResult<AppSuccess<()>> {
    databoard::update_data(databoard_id, databoard_data_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_data(
    Path((databoard_id, databoard_data_id)): Path<(String, String)>,
) -> AppResult<AppSuccess<()>> {
    databoard::delete_data(databoard_id, databoard_data_id).await?;
    Ok(AppSuccess::empty())
}

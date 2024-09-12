use axum::{
    extract::{Path, Query, State},
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
use uuid::Uuid;

use crate::{AppResult, AppState, AppSuccess};

pub fn routes() -> Router<AppState> {
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
    State(state): State<AppState>,
    Query(query): Query<QueryRuleInfo>,
) -> AppResult<AppSuccess<SearchRuleInfo>> {
    let rule_info = databoard::get_rule_info(&state.databoards, query).await?;
    Ok(AppSuccess::data(rule_info))
}

async fn create_databoard(
    State(state): State<AppState>,
    Json(req): Json<CreateUpdateDataboardReq>,
) -> AppResult<AppSuccess<()>> {
    databoard::create_databoard(&state.storage, &state.databoards, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_databoards(
    State(state): State<AppState>,
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchDataboardsResp>> {
    let resp =
        databoard::search_databoards(&state.storage, &state.databoards, pagination, query_params)
            .await?;
    Ok(AppSuccess::data(resp))
}

async fn update_databoard(
    State(state): State<AppState>,
    Path(databoard_id): Path<Uuid>,
    Json(req): Json<CreateUpdateDataboardReq>,
) -> AppResult<AppSuccess<()>> {
    databoard::update_databoard(&state.storage, &state.databoards, databoard_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_databoard(
    State(state): State<AppState>,
    Path(databoard_id): Path<Uuid>,
) -> AppResult<AppSuccess<()>> {
    databoard::delete_databoard(&state.storage, &state.databoards, databoard_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_data(
    State(state): State<AppState>,
    Path(databoard_id): Path<Uuid>,
    Json(req): Json<CreateUpdateDataReq>,
) -> AppResult<AppSuccess<()>> {
    databoard::create_data(
        &state.storage,
        &state.databoards,
        databoard_id,
        Uuid::new_v4(),
        req,
        true,
    )
    .await?;
    Ok(AppSuccess::empty())
}

async fn search_datas(
    State(state): State<AppState>,
    Path(databoard_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchDatasResp>> {
    let data = databoard::search_datas(&state.databoards, databoard_id, pagination, query).await?;
    Ok(AppSuccess::data(data))
}

async fn update_data(
    State(state): State<AppState>,
    Path((databoard_id, databoard_data_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateDataReq>,
) -> AppResult<AppSuccess<()>> {
    databoard::update_data(
        &state.storage,
        &state.databoards,
        databoard_id,
        databoard_data_id,
        req,
    )
    .await?;
    Ok(AppSuccess::empty())
}

async fn delete_data(
    State(state): State<AppState>,
    Path((databoard_id, databoard_data_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    databoard::delete_data(
        &state.storage,
        &state.databoards,
        databoard_id,
        databoard_data_id,
    )
    .await?;
    Ok(AppSuccess::empty())
}

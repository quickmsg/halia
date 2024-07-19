use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use common::error::HaliaError;
use devices::coap::manager::GLOBAL_COAP_MANAGER;
use types::devices::coap::{
    CreateUpdateCoapReq, CreateUpdateGroupAPIReq, CreateUpdateGroupReq, CreateUpdateSinkReq,
    SearchGroupAPIsResp, SearchGroupsResp, SearchSinksResp,
};
use uuid::Uuid;

use crate::{AppResp, DeleteIdsQuery, Pagination};

pub(crate) fn coap_routes() -> Router {
    Router::new()
        .route("/", post(create))
        .route("/:device_id", put(update))
        .route("/:device_id/start", put(start))
        .route("/:device_id/stop", put(stop))
        .route("/:device_id", routing::delete(delete))
        .nest(
            "/:device_id",
            Router::new()
                .nest(
                    "/group",
                    Router::new()
                        .route("/", post(create_group))
                        .route("/", get(search_groups))
                        .route("/:group_id", put(update_group))
                        .route("/:group_id", routing::delete(delete_group))
                        .nest(
                            "/:group_id/api",
                            Router::new()
                                .route("/", post(create_group_api))
                                .route("/", get(search_group_apis))
                                .route("/:api_id", put(update_group_api))
                                .route("/", routing::delete(delete_group_apis)),
                        ),
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

async fn create(Json(req): Json<CreateUpdateCoapReq>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.create(None, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn update(Path(device_id): Path<Uuid>, Json(req): Json<CreateUpdateCoapReq>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.update(device_id, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn start(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.start(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn stop(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.stop(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.delete(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_group(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateGroupReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.create_group(device_id, None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search_groups(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchGroupsResp> {
    match GLOBAL_COAP_MANAGER
        .search_groups(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(groups) => AppResp::with_data(groups),
        Err(e) => e.into(),
    }
}

async fn update_group(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER
        .update_group(device_id, group_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete_group(Path((device_id, group_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.delete_group(device_id, group_id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_group_api(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupAPIReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER
        .create_group_api(device_id, group_id, None, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search_group_apis(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    pagination: Query<Pagination>,
) -> AppResp<SearchGroupAPIsResp> {
    match GLOBAL_COAP_MANAGER
        .search_group_apis(device_id, group_id, pagination.p, pagination.s)
        .await
    {
        Ok(values) => AppResp::with_data(values),
        Err(e) => e.into(),
    }
}

async fn update_group_api(
    Path((device_id, group_id, api_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupAPIReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER
        .update_group_api(device_id, group_id, api_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn delete_group_apis(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Query(query): Query<DeleteIdsQuery>,
) -> AppResp<()> {
    let api_ids: Vec<Uuid> = match query
        .ids
        .split(',')
        .map(|s| s.parse::<Uuid>().map_err(|_e| return HaliaError::ParseErr))
        .collect::<Result<Vec<Uuid>, _>>()
    {
        Ok(ids) => ids,
        Err(e) => return e.into(),
    };

    match GLOBAL_COAP_MANAGER
        .delete_group_apis(device_id, group_id, api_ids)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn create_sink(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.create_sink(device_id, None, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn search_sinks(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSinksResp> {
    match GLOBAL_COAP_MANAGER
        .search_sinks(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub async fn update_sink(
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER
        .update_sink(device_id, sink_id, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub async fn delete_sink(Path((device_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_COAP_MANAGER.delete_sink(device_id, sink_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

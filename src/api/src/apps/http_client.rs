use apps::http_client::manager::GLOBAL_HTTP_CLIENT_MANAGER;
use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use types::{
    apps::http_client::{CreateUpdateHttpClientReq, CreateUpdateSinkReq, SearchSinksResp},
    Pagination,
};
use uuid::Uuid;

use crate::AppResp;

pub fn http_client_routes() -> Router {
    Router::new()
        .route("/", post(create))
        .route("/:app_id", put(update))
        .route("/:app_id", routing::delete(delete))
        .nest(
            "/:app_id",
            Router::new().nest(
                "/sink",
                Router::new()
                    .route("/", post(create_sink))
                    .route("/", get(search_sinks))
                    .route("/:sink_id", put(update_sink))
                    .route("/:sink_id", routing::delete(delete_sink)),
            ),
        )
}

async fn create(Json(req): Json<CreateUpdateHttpClientReq>) -> AppResp<()> {
    match GLOBAL_HTTP_CLIENT_MANAGER.create(None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn update(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateHttpClientReq>,
) -> AppResp<()> {
    match GLOBAL_HTTP_CLIENT_MANAGER.update(app_id, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete(Path(app_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_HTTP_CLIENT_MANAGER.delete(app_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_sink(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_HTTP_CLIENT_MANAGER
        .create_sink(app_id, None, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search_sinks(
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResp<SearchSinksResp> {
    match GLOBAL_HTTP_CLIENT_MANAGER
        .search_sinks(app_id, pagination)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

async fn update_sink(
    Path((app_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResp<()> {
    match GLOBAL_HTTP_CLIENT_MANAGER
        .update_sink(app_id, sink_id, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete_sink(Path((app_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_HTTP_CLIENT_MANAGER
        .delete_sink(app_id, sink_id)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

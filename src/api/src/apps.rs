use apps::GLOBAL_APP_MANAGER;
use axum::{
    body::Bytes,
    extract::{Path, Query},
};
use types::apps::{SearchConnectorResp, SearchSinkResp, SearchSourceResp};
use uuid::Uuid;

use crate::{AppResp, Pagination};

pub(crate) async fn create_connector(body: Bytes) -> AppResp<()> {
    match GLOBAL_APP_MANAGER.create_connector(&body).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_connectors(
    pagination: Query<Pagination>,
) -> AppResp<SearchConnectorResp> {
    AppResp::with_data(
        GLOBAL_APP_MANAGER
            .search_connectors(pagination.p, pagination.s)
            .await,
    )
}

pub(crate) async fn update_connector(Path(app_id): Path<Uuid>, body: Bytes) -> AppResp<()> {
    match GLOBAL_APP_MANAGER.update_connector(app_id, &body).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_connector(Path(app_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_APP_MANAGER.delete_connector(app_id).await {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}

pub(crate) async fn create_source(Path(app_id): Path<Uuid>, body: Bytes) -> AppResp<()> {
    match GLOBAL_APP_MANAGER.create_source(&app_id, &body).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_sources(
    Path(app_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSourceResp> {
    match GLOBAL_APP_MANAGER
        .search_source(&app_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_source(
    Path((app_id, source_id)): Path<(Uuid, Uuid)>,
    body: Bytes,
) -> AppResp<()> {
    match GLOBAL_APP_MANAGER
        .update_source(&app_id, source_id, &body)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_source(Path(app_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_APP_MANAGER.delete_connector(app_id).await {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}

pub(crate) async fn create_sink(Path(app_id): Path<Uuid>, body: Bytes) -> AppResp<()> {
    match GLOBAL_APP_MANAGER.create_sink(&app_id, &body).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_sinks(
    Path(app_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSinkResp> {
    match GLOBAL_APP_MANAGER
        .search_sinks(&app_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_sink(Path(app_id): Path<Uuid>, body: Bytes) -> AppResp<()> {
    match GLOBAL_APP_MANAGER.update_connector(app_id, &body).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_sink(Path(app_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_APP_MANAGER.delete_connector(app_id).await {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}

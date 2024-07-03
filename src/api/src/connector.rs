use axum::{
    body::Bytes,
    extract::{Path, Query},
};
use connectors::GLOBAL_CONNECTOR_MANAGER;
use types::connector::{SearchConnectorResp, SearchSinkResp, SearchSourceResp};
use uuid::Uuid;

use crate::{AppResp, Pagination};

pub(crate) async fn create_connector(body: Bytes) -> AppResp<()> {
    match GLOBAL_CONNECTOR_MANAGER.create_connector(&body).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_connectors(
    pagination: Query<Pagination>,
) -> AppResp<SearchConnectorResp> {
    AppResp::with_data(
        GLOBAL_CONNECTOR_MANAGER
            .search_connectors(pagination.p, pagination.s)
            .await,
    )
}

pub(crate) async fn update_connector(Path(connector_id): Path<Uuid>, body: Bytes) -> AppResp<()> {
    match GLOBAL_CONNECTOR_MANAGER
        .update_connector(connector_id, &body)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_connector(Path(connector_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_CONNECTOR_MANAGER
        .delete_connector(connector_id)
        .await
    {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}

pub(crate) async fn create_source(Path(connector_id): Path<Uuid>, body: Bytes) -> AppResp<()> {
    match GLOBAL_CONNECTOR_MANAGER
        .create_source(&connector_id, &body)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_sources(
    Path(connector_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSourceResp> {
    match GLOBAL_CONNECTOR_MANAGER
        .search_source(&connector_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_source(
    Path((connector_id, source_id)): Path<(Uuid, Uuid)>,
    body: Bytes,
) -> AppResp<()> {
    match GLOBAL_CONNECTOR_MANAGER
        .update_source(&connector_id, source_id, &body)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_source(Path(connector_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_CONNECTOR_MANAGER
        .delete_connector(connector_id)
        .await
    {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}

pub(crate) async fn create_sink(Path(connector_id): Path<Uuid>, body: Bytes) -> AppResp<()> {
    match GLOBAL_CONNECTOR_MANAGER
        .create_sink(&connector_id, &body)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search_sinks(
    Path(connector_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchSinkResp> {
    match GLOBAL_CONNECTOR_MANAGER
        .search_sinks(&connector_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_sink(Path(connector_id): Path<Uuid>, body: Bytes) -> AppResp<()> {
    match GLOBAL_CONNECTOR_MANAGER
        .update_connector(connector_id, &body)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_sink(Path(connector_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_CONNECTOR_MANAGER
        .delete_connector(connector_id)
        .await
    {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}

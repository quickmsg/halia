use axum::{extract::Path, Json};
use source::GLOBAL_SOURCE_MANAGER;
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

use crate::AppResp;

pub(crate) async fn create(Json(req): Json<CreateSourceReq>) -> AppResp<()> {
    match GLOBAL_SOURCE_MANAGER.create(None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn read(Path(id): Path<Uuid>) -> AppResp<SourceDetailResp> {
    match GLOBAL_SOURCE_MANAGER.read_source(id).await {
        Ok(resp) => AppResp::with_data(resp),
        Err(e) => e.into(),
    }
}

pub(crate) async fn list() -> AppResp<Vec<ListSourceResp>> {
    match GLOBAL_SOURCE_MANAGER.list().await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update_source() {}

pub(crate) async fn delete_source() {}

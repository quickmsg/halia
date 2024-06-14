use axum::{extract::Path, Json};
use sink::GLOBAL_SINK_MANAGER;
use types::sink::{CreateSinkReq, ListSinkResp, ReadSinkResp, UpdateSinkReq};
use uuid::Uuid;

use crate::AppResp;

pub(crate) async fn create(Json(req): Json<CreateSinkReq>) -> AppResp<()> {
    match GLOBAL_SINK_MANAGER.create(None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn read(Path(id): Path<Uuid>) -> AppResp<ReadSinkResp> {
    match GLOBAL_SINK_MANAGER.read(id).await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn list() -> AppResp<Vec<ListSinkResp>> {
    match GLOBAL_SINK_MANAGER.list().await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update(Path(id): Path<Uuid>, Json(req): Json<UpdateSinkReq>) -> AppResp<()> {
    match GLOBAL_SINK_MANAGER.update(id, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete(Path(id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_SINK_MANAGER.delete(id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

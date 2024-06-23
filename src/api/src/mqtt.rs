use axum::{
    extract::{Path, Query},
    Json,
};
use source::GLOBAL_SOURCE_MANAGER;
use tracing::trace;
use types::source::mqtt::{SearchTopicResp, TopicReq};
use uuid::Uuid;

use crate::{AppResp, Pagination};

pub(crate) async fn create(Path(mqtt_id): Path<Uuid>, Json(req): Json<TopicReq>) -> AppResp<()> {
    trace!("create_device:{:?}", req);
    match GLOBAL_SOURCE_MANAGER.create_topic(mqtt_id, None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn search(
    Path(mqtt_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchTopicResp> {
    match GLOBAL_SOURCE_MANAGER
        .search_topic(mqtt_id, pagination.p, pagination.s)
        .await
    {
        Ok(resp) => AppResp::with_data(resp),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update(
    Path((mqtt_id, topic_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<TopicReq>,
) -> AppResp<()> {
    match GLOBAL_SOURCE_MANAGER
        .update_topic(mqtt_id, topic_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete(Path((mqtt_id, topic_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_SOURCE_MANAGER.delete_topic(mqtt_id, topic_id).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

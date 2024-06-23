use axum::{
    extract::{Path, Query},
    Json,
};
use source::GLOBAL_SOURCE_MANAGER;
use tracing::trace;
use types::{device::point::SearchPointResp, source::mqtt::TopicReq};
use uuid::Uuid;

use crate::{AppResp, Pagination};

pub(crate) async fn create(Path(mqtt_id): Path<Uuid>, Json(req): Json<TopicReq>) -> AppResp<()> {
    trace!("create_device:{:?}", req);
    match GLOBAL_SOURCE_MANAGER.create_topic(mqtt_id, None, req).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

// pub(crate) async fn search(pagination: Query<Pagination>) -> AppResp<SearchPointResp> {
//     AppResp::with_data(
//         GLOBAL_SOURCE_MANAGER
//             .search_topic(pagination.p, pagination.s)
//             .await,
//     )
// }

// pub(crate) async fn update(Path(device_id): Path<Uuid>) -> AppResp<()> {
//     match GLOBAL_SOURCE_MANAGER.start_device(device_id).await {
//         Ok(()) => AppResp::new(),
//         Err(e) => e.into(),
//     }
// }

// pub(crate) async fn delete(Path(device_id): Path<Uuid>) -> AppResp<()> {
//     match GLOBAL_SOURCE_MANAGER.start_device(device_id).await {
//         Ok(()) => AppResp::new(),
//         Err(e) => e.into(),
//     }
// }

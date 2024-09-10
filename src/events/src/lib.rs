//! 事件类型主要用于记录主要资源（设备、应用）等的启动停止和异常以及恢复，方便客户排查系统问题。
use std::{str::FromStr, sync::Arc};

use common::storage;
use sqlx::AnyPool;
use tracing::warn;
use types::{
    events::{EventType, QueryParams, SearchEventsItemResp, SearchEventsResp},
    Pagination,
};
use uuid::Uuid;

// pub async fn create_event(
//     storage: &Arc<AnyPool>,
//     id: &Uuid,
//     source_type: SourceType,
//     event_type: EventType,
//     info: Option<String>,
// ) {
//     if let Err(e) =
//         storage::device::create_event(storage, id, source_type.into(), event_type.into(), info)
//             .await
//     {
//         warn!("create event failed: {}", e);
//     }
// }

// pub async fn search_events(
//     storage: &Arc<AnyPool>,
//     query_params: QueryParams,
//     pagination: Pagination,
// ) -> SearchEventsResp {
//     match storage::event::search_events(storage, query_params, pagination).await {
//         Ok((events_db, total)) => {
//             let mut events_resp = vec![];
//             for event in events_db {
//                 let id: Uuid = Uuid::from_str(&event.id).unwrap();
//                 let source_type: SourceType = event.source_type.try_into().unwrap();
//                 match source_type {
//                     SourceType::Device => todo!(),
//                     SourceType::App => todo!(),
//                     SourceType::Rule => todo!(),
//                 }
//                 // todo get name from devices or apps or rules
//                 events_resp.push(SearchEventsItemResp {
//                     id,
//                     name: "xx".to_owned(),
//                     source_type,
//                     event_type: event.event_type.try_into().unwrap(),
//                     info: event.info,
//                 });
//             }

//             SearchEventsResp {
//                 total: total as usize,
//                 data: events_resp,
//             }
//         }
//         Err(e) => {
//             warn!("search events failed: {}", e);
//             SearchEventsResp {
//                 total: 0,
//                 data: vec![],
//             }
//         }
//     }
// }

pub async fn delete_events(pool: &Arc<AnyPool>, id: &Uuid) {}

//! 事件类型主要用于记录主要资源（设备、应用）等的启动停止和异常以及恢复，方便客户排查系统问题。
use std::sync::Arc;

use anyhow::Result;
use common::storage;
use sqlx::AnyPool;
use tracing::warn;
use types::{
    events::{QueryParams, SearchEventsResp},
    Pagination,
};
use uuid::Uuid;

pub enum SourceType {
    Device,
    App,
    Rule,
}

impl Into<i32> for SourceType {
    fn into(self) -> i32 {
        match self {
            SourceType::Device => 1,
            SourceType::App => 2,
            SourceType::Rule => 3,
        }
    }
}

pub enum EventType {
    // 启动
    Start,
    // 停止
    Stop,
    // 连接成功
    Connect,
    // 连接断开
    DisConnect,
}

impl Into<i32> for EventType {
    fn into(self) -> i32 {
        match self {
            EventType::Start => 1,
            EventType::Stop => 2,
            EventType::Connect => 3,
            EventType::DisConnect => 4,
        }
    }
}

pub async fn create_event(
    storage: &Arc<AnyPool>,
    id: &Uuid,
    source_type: SourceType,
    event_type: EventType,
    info: Option<String>,
) {
    if let Err(e) =
        storage::event::create_event(storage, id, source_type.into(), event_type.into(), info).await
    {
        warn!("create event failed: {}", e);
    }
}

pub async fn search_events(
    storage: &Arc<AnyPool>,
    query_params: QueryParams,
    pagination: Pagination,
) -> SearchEventsResp {
    match storage::event::search_events(storage, query_params, pagination).await {
        Ok((events, total)) => todo!(),
        Err(e) => {
            warn!("search events failed: {}", e);
            SearchEventsResp {
                total: 0,
                data: vec![],
            }
        }
    }
}

pub async fn delete_events(pool: &Arc<AnyPool>, id: &Uuid) {}

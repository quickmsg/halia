//! 事件类型主要用于记录主要资源（设备、应用）等的启动停止和异常以及恢复，方便客户排查系统问题。
use std::sync::Arc;

use sqlx::AnyPool;
use uuid::Uuid;

pub enum SourceType {
    Device,
    App,
    Rule,
}

pub enum EventType {
    // 启动
    On,
    // 停止
    Close,
    // 异常
    Error,
    // 异常恢复
    Recover,
}

pub async fn create_event(
    pool: &Arc<AnyPool>,
    id: &Uuid,
    name: String,
    source_type: SourceType,
    event_type: EventType,
    info: Option<String>,
) {
}

pub async fn search_events(pool: &Arc<AnyPool>) {}

pub async fn delete_events(pool: &Arc<AnyPool>, id: &Uuid) {}

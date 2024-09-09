//! 事件类型主要用于记录主要资源（设备、应用）等的启动停止和异常以及恢复，方便客户排查系统问题。
use std::sync::Arc;

use anyhow::Result;
use common::storage;
use sqlx::AnyPool;
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
    On,
    // 停止
    Close,
    // 异常
    Error,
    // 异常恢复
    Recover,
}

impl Into<i32> for EventType {
    fn into(self) -> i32 {
        match self {
            EventType::On => 1,
            EventType::Close => 2,
            EventType::Error => 3,
            EventType::Recover => 4,
        }
    }
}

pub async fn create_event(
    pool: &Arc<AnyPool>,
    id: &Uuid,
    source_type: SourceType,
    event_type: EventType,
    info: Option<String>,
) -> Result<()> {
    storage::event::create_event(pool, id, source_type.into(), event_type.into(), info).await?;

    Ok(())
}

pub async fn search_events(pool: &Arc<AnyPool>) {}

pub async fn delete_events(pool: &Arc<AnyPool>, id: &Uuid) {}
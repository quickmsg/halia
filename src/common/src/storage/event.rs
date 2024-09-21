use anyhow::Result;
use sqlx::FromRow;
use types::{
    events::{EventType, QueryParams, ResourceType},
    Pagination,
};

use super::POOL;

pub struct Event {
    pub db_event: DbEvent,
    pub name: String,
}

#[derive(FromRow)]
pub struct DbEvent {
    pub resource_type: String,
    pub resource_id: String,
    pub event_type: String,
    pub info: Option<String>,
    pub ts: i64,
}

// 考虑在更新设备名称的时候，更新此表
pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS events (
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    typ TEXT NOT NULL,
    info TEXT,
    ts INTEGER NOT NULL
)
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn insert(
    resource_type: ResourceType,
    resource_id: &String,
    typ: EventType,
    info: Option<String>,
) -> Result<()> {
    let resource_type: String = resource_type.into();
    let typ: String = typ.into();
    let ts = chrono::Utc::now().timestamp();
    match info {
        Some(info) => {
            sqlx::query(
                "INSERT INTO events (resource_type, resource_id, event_type, info, ts) VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(resource_type)
            .bind(resource_id)
            .bind(typ)
            .bind(info)
            .bind(ts)
            .execute(POOL.get().unwrap())
            .await?;
        }
        None => {
            sqlx::query("INSERT INTO events (resource_type, resource_id, event_type, ts) VALUES (?1, ?2, ?3, ?4)")
            .bind(resource_type)
            .bind(resource_id)
            .bind(typ)
            .bind(ts)
            .execute(POOL.get().unwrap())
                .await?;
        }
    }

    Ok(())
}

pub async fn query(pagination: Pagination, query: QueryParams) -> Result<(Vec<Event>, usize)> {
    let offset = (pagination.page - 1) * pagination.size;
    let (mut db_events, count) = match (query.typ, query.resource_type) {
        (None, None) => {
            let events = sqlx::query_as::<_, DbEvent>(
                "SELECT * FROM events ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
                .fetch_one(POOL.get().unwrap())
                .await?;

            (events, count)
        }
        (None, Some(resource_type)) => {
            let resource_type: String = resource_type.into();
            let events = sqlx::query_as::<_, DbEvent>(
                "SELECT * FROM events WHERE resource_type = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(&resource_type)
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE resource_type = ?1")
                    .bind(&resource_type)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            (events, count)
        }
        (Some(typ), None) => {
            let typ: String = typ.into();
            let events = sqlx::query_as::<_, DbEvent>(
                "SELECT * FROM events WHERE typ = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(&typ)
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE typ = ?1")
                .bind(&typ)
                .fetch_one(POOL.get().unwrap())
                .await?;

            (events, count)
        }
        (Some(typ), Some(resource_type)) => {
            let typ: String = typ.into();
            let resource_type: String = resource_type.into();
            let events = sqlx::query_as::<_, DbEvent>(
                "SELECT * FROM events WHERE typ = ?1 AND resource_type = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(&typ)
            .bind(&resource_type)
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE typ = ?1 AND resource_type = ?2",
            )
            .bind(&typ)
            .bind(&resource_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            (events, count)
        }
    };

    let mut events: Vec<Event> = Vec::with_capacity(db_events.len());

    if let Some(db_event) = db_events.pop() {
        let resource_type: ResourceType =
            ResourceType::try_from(db_event.resource_type.as_str()).unwrap();
        match resource_type {
            ResourceType::Device => {
                let name: String = sqlx::query_scalar("SELECT name FROM devices WHERE id = ?1")
                    .bind(&db_event.resource_id)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

                events.push(Event { db_event, name });
            }
            ResourceType::App => {
                let name: String = sqlx::query_scalar("SELECT name FROM apps WHERE id = ?1")
                    .bind(&db_event.resource_id)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

                events.push(Event { db_event, name });
            }
            ResourceType::Databoard => {
                let name: String = sqlx::query_scalar("SELECT name FROM databoards WHERE id = ?1")
                    .bind(&db_event.resource_id)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

                events.push(Event { db_event, name });
            }
            ResourceType::Rule => {
                let name: String = sqlx::query_scalar("SELECT name FROM rules WHERE id = ?1")
                    .bind(&db_event.resource_id)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

                events.push(Event { db_event, name });
            }
        }
    }

    Ok((events, count as usize))
}

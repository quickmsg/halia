use anyhow::Result;
use sqlx::FromRow;
use types::{
    events::{EventType, QueryParams, ResourceType},
    Pagination,
};

use super::POOL;

#[derive(FromRow)]
pub struct Event {
    pub resource_type: String,
    pub resource_name: String,
    pub typ: String,
    pub info: Option<String>,
    pub ts: i64,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS events (
    resource_type VARCHAR(255) NOT NULL,
    resource_name VARCHAR(255) NOT NULL,
    typ VARCHAR(255) NOT NULL,           -- 类型字段使用 VARCHAR(255)
    info TEXT,                           -- 信息字段使用 TEXT 类型
    ts INTEGER NOT NULL                  -- 时间戳字段使用 INTEGER
);
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
    let resource_name = match resource_type {
        ResourceType::Device => {
            let name: String = sqlx::query_scalar("SELECT name FROM devices WHERE id = ?1")
                .bind(resource_id)
                .fetch_one(POOL.get().unwrap())
                .await?;
            name
        }
        ResourceType::App => {
            let name: String = sqlx::query_scalar("SELECT name FROM apps WHERE id = ?1")
                .bind(resource_id)
                .fetch_one(POOL.get().unwrap())
                .await?;
            name
        }
        ResourceType::Databoard => {
            let name: String = sqlx::query_scalar("SELECT name FROM databoards WHERE id = ?1")
                .bind(resource_id)
                .fetch_one(POOL.get().unwrap())
                .await?;
            name
        }
        ResourceType::Rule => {
            let name: String = sqlx::query_scalar("SELECT name FROM rules WHERE id = ?1")
                .bind(resource_id)
                .fetch_one(POOL.get().unwrap())
                .await?;
            name
        }
    };

    let resource_type: String = resource_type.into();
    let typ: String = typ.into();
    let ts = chrono::Utc::now().timestamp();
    match info {
        Some(info) => {
            sqlx::query(
                "INSERT INTO events (resource_type, resource_name, typ, info, ts) VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(resource_type)
            .bind(resource_name)
            .bind(typ)
            .bind(info)
            .bind(ts)
            .execute(POOL.get().unwrap())
            .await?;
        }
        None => {
            sqlx::query("INSERT INTO events (resource_type, resource_name, typ, ts) VALUES (?1, ?2, ?3, ?4)")
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
    let (events, count) = match (query.name, query.typ, query.resource_type) {
        (None, None, None) => {
            let events = sqlx::query_as::<_, Event>(
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
        (None, None, Some(resource_type)) => {
            let resource_type: String = resource_type.into();
            let events = sqlx::query_as::<_, Event>(
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
        (None, Some(typ), None) => {
            let typ: String = typ.into();
            let events = sqlx::query_as::<_, Event>(
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
        (None, Some(typ), Some(resource_type)) => {
            let typ: String = typ.into();
            let resource_type: String = resource_type.into();
            let events = sqlx::query_as::<_, Event>(
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
        (Some(name), None, None) => {
            let name = format!("%{}%", name);
            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE resource_name LIKE ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(&name)
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE resource_name LIKE ?1")
                    .bind(&name)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            (events, count)
        }
        (Some(name), None, Some(resource_type)) => {
            let name = format!("%{}%", name);
            let resource_type: String = resource_type.into();
            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE resource_name LIKE ?1 AND resource_type = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(&name)
            .bind(&resource_type)
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE resource_name LIKE ?1 AND resource_type = ?2",
            )
            .bind(&name)
            .bind(&resource_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            (events, count)
        }
        (Some(name), Some(typ), None) => {
            let name = format!("%{}%", name);
            let typ: String = typ.into();
            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE resource_name LIKE ?1 AND typ = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(&name)
            .bind(&typ)
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE resource_name LIKE ?1 AND typ = ?2",
            )
            .bind(&name)
            .bind(&typ)
            .fetch_one(POOL.get().unwrap())
            .await?;

            (events, count)
        }
        (Some(name), Some(typ), Some(resource_type)) => {
            let name = format!("%{}%", name);
            let typ: String = typ.into();
            let resource_type: String = resource_type.into();
            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE resource_name LIKE ?1 AND typ = ?2 AND resource_type = ?3 ORDER BY ts DESC LIMIT ?4 OFFSET ?5",
            )
            .bind(&name)
            .bind(&typ)
            .bind(&resource_type)
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE resource_name LIKE ?1 AND typ = ?2 AND resource_type = ?3",
            )
            .bind(&name)
            .bind(&typ)
            .bind(&resource_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            (events, count)
        }
    };

    Ok((events, count as usize))
}

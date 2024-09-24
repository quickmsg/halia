use anyhow::Result;
use sqlx::FromRow;
use types::{
    events::{EventType, QueryParams, ResourceType},
    Pagination,
};

use super::POOL;

#[derive(FromRow)]
pub struct Event {
    pub resource_type: i32,
    pub resource_name: String,
    pub typ: i32,
    pub info: Option<Vec<u8>>,
    pub ts: i64,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS events (
    resource_type SMALLINT UNSIGNED NOT NULL,
    resource_name VARCHAR(255) NOT NULL,
    typ SMALLINT UNSIGNED NOT NULL,
    info BLOB,
    ts INTEGER UNSIGNED NOT NULL
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
            let name: String = sqlx::query_scalar("SELECT name FROM devices WHERE id = ?")
                .bind(resource_id)
                .fetch_one(POOL.get().unwrap())
                .await?;
            name
        }
        ResourceType::App => {
            let name: String = sqlx::query_scalar("SELECT name FROM apps WHERE id = ?")
                .bind(resource_id)
                .fetch_one(POOL.get().unwrap())
                .await?;
            name
        }
        ResourceType::Databoard => {
            let name: String = sqlx::query_scalar("SELECT name FROM databoards WHERE id = ?")
                .bind(resource_id)
                .fetch_one(POOL.get().unwrap())
                .await?;
            name
        }
        ResourceType::Rule => {
            let name: String = sqlx::query_scalar("SELECT name FROM rules WHERE id = ?")
                .bind(resource_id)
                .fetch_one(POOL.get().unwrap())
                .await?;
            name
        }
    };

    let resource_type: i32 = resource_type.into();
    let typ: i32 = typ.into();
    let info = info.map(|info| info.into_bytes());
    let ts = chrono::Utc::now().timestamp();
    sqlx::query(
        "INSERT INTO events (resource_type, resource_name, typ, info, ts) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(resource_type)
    .bind(resource_name)
    .bind(typ)
    .bind(info)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Event>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, events) = match (
        query_params.name,
        query_params.typ,
        query_params.resource_type,
    ) {
        (None, None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
                .fetch_one(POOL.get().unwrap())
                .await?;

            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, events)
        }
        (None, None, Some(resource_type)) => {
            let resource_type: i32 = resource_type.into();

            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE resource_type = ?")
                    .bind(resource_type)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE resource_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(resource_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, events)
        }
        (None, Some(typ), None) => {
            let typ: i32 = typ.into();

            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE typ = ?")
                .bind(typ)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE typ = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(typ)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, events)
        }
        (None, Some(typ), Some(resource_type)) => {
            let typ: i32 = typ.into();
            let resource_type: i32 = resource_type.into();

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE typ = ? AND resource_type = ?",
            )
            .bind(typ)
            .bind(resource_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let events = sqlx::query_as::<_, Event>(
                    "SELECT * FROM events WHERE typ = ? AND resource_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                )
                .bind(typ)
                .bind(resource_type)
                .bind(limit)
                .bind(offset)
                .fetch_all(POOL.get().unwrap())
                .await?;

            (count, events)
        }
        (Some(name), None, None) => {
            let name = format!("%{}%", name);

            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE resource_name LIKE ?")
                    .bind(&name)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE resource_name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, events)
        }
        (Some(name), None, Some(resource_type)) => {
            let name = format!("%{}%", name);
            let resource_type: i32 = resource_type.into();

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE resource_name LIKE ? AND resource_type = ?",
            )
            .bind(&name)
            .bind(resource_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE resource_name LIKE ? AND resource_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(resource_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, events)
        }
        (Some(name), Some(typ), None) => {
            let name = format!("%{}%", name);
            let typ: i32 = typ.into();

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE resource_name LIKE ? AND typ = ?",
            )
            .bind(&name)
            .bind(typ)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE resource_name LIKE ? AND typ = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, events)
        }
        (Some(name), Some(typ), Some(resource_type)) => {
            let name = format!("%{}%", name);
            let typ: i32 = typ.into();
            let resource_type: i32 = resource_type.into();

            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM events WHERE resource_name LIKE ? AND typ = ? AND resource_type = ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(resource_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let events = sqlx::query_as::<_, Event>(
                "SELECT * FROM events WHERE resource_name LIKE ? AND typ = ? AND resource_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(resource_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, events)
        }
    };

    Ok((count as usize, events))
}

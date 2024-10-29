use anyhow::Result;
use sqlx::{
    any::AnyArguments,
    query::{QueryAs, QueryScalar},
    Any, FromRow,
};
use types::{
    events::{EventType, QueryParams, ResourceType},
    Pagination,
};

use super::POOL;

static TABLE_NAME: &str = "events";

#[derive(FromRow)]
pub struct Event {
    pub resource_type: i32,
    pub resource_name: String,
    pub typ: i32,
    pub info: Option<Vec<u8>>,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    resource_type SMALLINT UNSIGNED NOT NULL,
    resource_name VARCHAR(255) NOT NULL,
    typ SMALLINT UNSIGNED NOT NULL,
    info BLOB,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
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
    let ts = common::timestamp_millis() as i64;
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
        &query_params.name,
        &query_params.typ,
        &query_params.resource_type,
        &query_params.begin_ts,
        &query_params.end_ts,
    ) {
        (None, None, None, None, None) => {
            let count: i64 =
                sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let events = sqlx::query_as::<_, Event>(
                format!(
                    "SELECT * FROM {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, events)
        }
        _ => {
            let mut where_clause = String::new();
            if query_params.name.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE resource_name LIKE ?"),
                    false => where_clause.push_str(" AND resource_name LIKE ?"),
                }
            }
            if query_params.resource_type.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE resource_type = ?"),
                    false => where_clause.push_str(" AND resource_type = ?"),
                }
            }
            if query_params.typ.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE typ = ?"),
                    false => where_clause.push_str(" AND typ = ?"),
                }
            }
            if query_params.begin_ts.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE ts >= ?"),
                    false => where_clause.push_str(" AND ts >= ?"),
                }
            }
            if query_params.end_ts.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE ts <= ?"),
                    false => where_clause.push_str(" AND ts <= ?"),
                }
            }

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(&query_count_str);

            let query_schemas_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );
            let mut query_schemas_builder: QueryAs<'_, Any, Event, AnyArguments> =
                sqlx::query_as::<_, Event>(&query_schemas_str);

            if let Some(name) = query_params.name {
                let name = format!("%{}%", name);
                query_count_builder = query_count_builder.bind(name.clone());
                query_schemas_builder = query_schemas_builder.bind(name);
            }
            if let Some(resource_type) = query_params.resource_type {
                let typ: i32 = resource_type.into();
                query_count_builder = query_count_builder.bind(typ);
                query_schemas_builder = query_schemas_builder.bind(typ);
            }
            if let Some(typ) = query_params.typ {
                let typ: i32 = typ.into();
                query_count_builder = query_count_builder.bind(typ);
                query_schemas_builder = query_schemas_builder.bind(typ);
            }
            if let Some(begin_ts) = query_params.begin_ts {
                query_count_builder = query_count_builder.bind(begin_ts);
                query_schemas_builder = query_schemas_builder.bind(begin_ts);
            }
            if let Some(end_ts) = query_params.end_ts {
                query_count_builder = query_count_builder.bind(end_ts);
                query_schemas_builder = query_schemas_builder.bind(end_ts);
            }

            let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
            let events = query_schemas_builder
                .bind(limit)
                .bind(offset)
                .fetch_all(POOL.get().unwrap())
                .await?;

            (count, events)
        }
    };

    Ok((count as usize, events))
}

pub async fn delete_expired(day: usize) -> Result<()> {
    let ts = common::timestamp_millis() as i64;
    sqlx::query("DELETE FROM events WHERE ts < ?")
        .bind(ts - (day as i64) * 24 * 60 * 60 * 1000)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

use anyhow::Result;
use sqlx::FromRow;
use types::{CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams};

use crate::timestamp_millis;

use super::POOL;

pub enum Type {
    Source,
    Sink,
}

impl From<i32> for Type {
    fn from(i: i32) -> Self {
        match i {
            1 => Type::Source,
            2 => Type::Sink,
            _ => panic!("invalid type"),
        }
    }
}

impl Into<i32> for Type {
    fn into(self) -> i32 {
        match self {
            Type::Source => 1,
            Type::Sink => 2,
        }
    }
}

#[derive(FromRow)]
pub struct SourceOrSink {
    pub id: String,
    pub typ: i32,
    pub parent_id: String,
    pub name: String,
    pub des: Option<Vec<u8>>,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS sources_or_sinks (
    id CHAR(32) PRIMARY KEY,
    typ SMALLINT UNSIGNED NOT NULL,
    parent_id CHAR(32) NOT NULL,
    name VARCHAR(255) NOT NULL,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn insert_name_exists(parent_id: &String, typ: Type, name: &String) -> Result<bool> {
    let typ: i32 = typ.into();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sources_or_sinks WHERE parent_id = ? AND name = ? AND typ = ?",
    )
    .bind(parent_id)
    .bind(name)
    .bind(typ)
    .fetch_one(POOL.get().unwrap())
    .await?;

    Ok(count > 0)
}

pub async fn update_name_exists(
    parent_id: &String,
    typ: Type,
    id: &String,
    name: &String,
) -> Result<bool> {
    let typ: i32 = typ.into();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sources_or_sinks WHERE parent_id = ? AND name = ? AND typ = ? AND id != ?",
    )
    .bind(parent_id)
    .bind(name)
    .bind(typ)
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    Ok(count > 0)
}

pub async fn insert(
    parent_id: &String,
    id: &String,
    typ: Type,
    req: CreateUpdateSourceOrSinkReq,
) -> Result<()> {
    let typ: i32 = typ.into();
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = timestamp_millis();
    let desc = match req.base.desc {
        Some(desc) => Some(desc.as_bytes().to_vec()),
        None => None,
    };

    sqlx::query(
                r#"INSERT INTO sources_or_sinks (id, typ, parent_id, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?)"#,
            )
        .bind(id)
        .bind(typ)
        .bind(parent_id)
        .bind(req.base.name)
        .bind(desc)
        .bind(conf)
        .bind(ts)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn read_all_by_parent_id(parent_id: &String, typ: Type) -> Result<Vec<SourceOrSink>> {
    let typ: i32 = typ.into();
    let sources_or_sinks = sqlx::query_as::<_, SourceOrSink>(
        "SELECT * FROM sources_or_sinks WHERE parent_id = ? AND typ = ? ORDER BY ts DESC",
    )
    .bind(parent_id)
    .bind(typ)
    .fetch_all(POOL.get().unwrap())
    .await?;
    Ok(sources_or_sinks)
}

pub async fn read_one(id: &String) -> Result<SourceOrSink> {
    let source_or_sink =
        sqlx::query_as::<_, SourceOrSink>("SELECT * FROM sources_or_sinks WHERE id = ?")
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;
    Ok(source_or_sink)
}

pub async fn query_by_parent_id(
    parent_id: &String,
    typ: Type,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> Result<(usize, Vec<SourceOrSink>)> {
    let typ: i32 = typ.into();
    let (count, sources_or_sinks) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sources_or_sinks WHERE parent_id = ? AND typ = ? AND name LIKE ?",
            )
            .bind(parent_id)
            .bind(typ)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, SourceOrSink>(
                "SELECT * FROM sources_or_sinks WHERE parent_id = ? AND typ = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            ).bind(parent_id)
            .bind(typ)
            .bind(format!("%{}%", name))
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap()).await?;

            (count, sources_or_sinks)
        }
        None => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sources_or_sinks WHERE parent_id = ? AND typ = ?",
            )
            .bind(parent_id)
            .bind(typ)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, SourceOrSink>(
                "SELECT * FROM sources_or_sinks WHERE parent_id = ? AND typ = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(parent_id)
            .bind(typ)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, sources_or_sinks)
        }
    };

    Ok((count as usize, sources_or_sinks))
}

pub async fn count_by_parent_id(parent_id: &String, typ: Type) -> Result<usize> {
    let typ: i32 = typ.into();
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sources_or_sinks WHERE parent_id = ? AND typ = ?")
            .bind(parent_id)
            .bind(typ)
            .fetch_one(POOL.get().unwrap())
            .await?;
    Ok(count as usize)
}

pub async fn read_conf(id: &String) -> Result<serde_json::Value> {
    let conf: Vec<u8> = sqlx::query_scalar("SELECT conf FROM sources_or_sinks WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;
    Ok(serde_json::from_slice(&conf)?)
}

pub async fn update(id: &String, req: CreateUpdateSourceOrSinkReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    let desc = match req.base.desc {
        Some(desc) => Some(desc.as_bytes().to_vec()),
        None => None,
    };
    sqlx::query("UPDATE sources_or_sinks SET name = ?, des = ?, conf = ? WHERE id = ?")
        .bind(req.base.name)
        .bind(desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete(id: &String) -> Result<()> {
    sqlx::query("DELETE FROM sources_or_sinks WHERE id = ?")
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete_by_parent_id(parent_id: &String) -> Result<()> {
    sqlx::query("DELETE FROM sources_or_sinks WHERE parent_id = ?")
        .bind(parent_id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn check_exists(id: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sources_or_sinks WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count == 1)
}

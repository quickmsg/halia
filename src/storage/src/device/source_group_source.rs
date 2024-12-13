use anyhow::Result;
use common::error::HaliaResult;
use sqlx::FromRow;
use tracing::debug;
use types::{
    devices::source_group::{CreateUpdateSourceReq, SourceQueryParams},
    Pagination,
};

use crate::POOL;

static TABLE_NAME: &str = "device_source_group_sources";

#[derive(FromRow)]
struct DbSource {
    pub id: String,
    pub source_group_id: String,
    pub name: String,
    pub conf: Vec<u8>,
    pub ts: i64,
}

impl DbSource {
    pub fn transfer(self) -> Result<Source> {
        Ok(Source {
            id: self.id,
            source_group_id: self.source_group_id,
            name: self.name,
            conf: serde_json::from_slice(&self.conf)?,
            ts: self.ts,
        })
    }
}

#[derive(Debug)]
pub struct Source {
    pub id: String,
    pub source_group_id: String,
    pub name: String,
    pub conf: serde_json::Value,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    source_group_id CHAR(32) NOT NULL,
    name VARCHAR(255) NOT NULL,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL,
    UNIQUE (source_group_id, name)
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(
    id: &String,
    source_group_id: &String,
    req: CreateUpdateSourceReq,
) -> Result<()> {
    sqlx::query(
        format!(
            r#"INSERT INTO {} 
            (id, source_group_id, name, conf, ts) 
            VALUES (?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(source_group_id)
    .bind(req.name)
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn get_by_id(id: &String) -> Result<Source> {
    let db_source = sqlx::query_as::<_, DbSource>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_source.transfer()
}

pub async fn read_by_source_group_id(source_group_id: &String) -> Result<Vec<Source>> {
    let db_sources = sqlx::query_as::<_, DbSource>(
        format!("SELECT * FROM {} WHERE source_group_id = ?", TABLE_NAME).as_str(),
    )
    .bind(source_group_id)
    .fetch_all(POOL.get().unwrap())
    .await?;

    db_sources
        .into_iter()
        .map(|db_source| db_source.transfer())
        .collect()
}

pub async fn search(
    source_group_id: &String,
    pagination: Pagination,
    query: SourceQueryParams,
) -> Result<(usize, Vec<Source>)> {
    debug!(
        "search source_group_id: {:?}, query: {:?}",
        source_group_id, query
    );
    let (limit, offset) = pagination.to_sql();
    let (count, db_sources) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE source_group_id = ? AND name LIKE ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_group_id)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let db_sources = sqlx::query_as::<_, DbSource>(
                format!(
                    "SELECT * FROM {} WHERE source_group_id = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_group_id)
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, db_sources)
        }
        None => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE source_group_id = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_group_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let db_sources = sqlx::query_as::<_, DbSource>(
                format!(
                    "SELECT * FROM {} WHERE source_group_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_group_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, db_sources)
        }
    };

    let sources = db_sources
        .into_iter()
        .map(|db| db.transfer())
        .collect::<Result<Vec<_>>>()?;

    Ok((count as usize, sources))
}

pub async fn count_by_source_group_id(source_group_id: &String) -> Result<usize> {
    let count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM {} WHERE source_group_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(source_group_id)
    .fetch_one(POOL.get().unwrap())
    .await?;
    Ok(count as usize)
}

pub async fn read_conf(id: &String) -> Result<serde_json::Value> {
    let conf: Vec<u8> =
        sqlx::query_scalar(format!("SELECT conf FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;
    Ok(serde_json::from_slice(&conf)?)
}

pub async fn update(id: &String, req: CreateUpdateSourceReq) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET name = ?, conf = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(serde_json::to_vec(&req.conf)?)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub(crate) async fn delete_many_by_device_template_id(
    device_template_id: &String,
) -> HaliaResult<()> {
    sqlx::query(format!("DELETE FROM {} WHERE device_template_id = ?", TABLE_NAME).as_str())
        .bind(device_template_id)
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

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    crate::delete_by_id(id, TABLE_NAME).await
}

pub async fn count_by_template_id(template_id: &String) -> Result<usize> {
    let count: i64 = sqlx::query_scalar(
        format!("SELECT COUNT(*) FROM {} WHERE template_id = ?", TABLE_NAME).as_str(),
    )
    .bind(template_id)
    .fetch_one(POOL.get().unwrap())
    .await?;
    Ok(count as usize)
}

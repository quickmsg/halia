// 设备模板下的源和动作
use anyhow::Result;
use common::error::HaliaResult;
use sqlx::FromRow;
use types::{
    devices::{device_template::source_sink::QueryParams, SourceSinkCreateUpdateReq},
    Pagination,
};

use crate::{SourceSinkType, POOL};

static TABLE_NAME: &str = "device_template_sources_sinks";

#[derive(FromRow)]
struct DbSourceSink {
    pub id: String,
    pub device_template_id: String,
    pub name: String,
    pub conf: Vec<u8>,
    pub ts: i64,
}

impl DbSourceSink {
    pub fn transfer(self) -> Result<SourceSink> {
        Ok(SourceSink {
            id: self.id,
            device_template_id: self.device_template_id,
            name: self.name,
            conf: serde_json::from_slice(&self.conf)?,
            ts: self.ts,
        })
    }
}

#[derive(Debug)]
pub struct SourceSink {
    pub id: String,
    pub device_template_id: String,
    pub name: String,
    pub conf: serde_json::Value,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    device_template_id CHAR(32) NOT NULL,
    source_sink_type SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    conf_type SMALLINT UNSIGNED NOT NULL,
    template_id CHAR(32),
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL,
    UNIQUE (device_template_id, source_sink_type, name)
);
"#,
        TABLE_NAME
    )
}

pub async fn insert_source(
    id: &String,
    device_template_id: &String,
    req: SourceSinkCreateUpdateReq,
) -> Result<()> {
    insert(SourceSinkType::Source, id, device_template_id, req).await
}

pub async fn insert_sink(
    id: &String,
    device_template_id: &String,
    req: SourceSinkCreateUpdateReq,
) -> Result<()> {
    insert(SourceSinkType::Sink, id, device_template_id, req).await
}

async fn insert(
    source_sink_type: SourceSinkType,
    id: &String,
    device_template_id: &String,
    req: SourceSinkCreateUpdateReq,
) -> Result<()> {
    sqlx::query(
        format!(
            r#"INSERT INTO {} 
            (id, device_template_id, source_sink_type, name, conf_type, template_id, conf, ts) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(device_template_id)
    .bind(Into::<i32>::into(source_sink_type))
    .bind(req.name)
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<SourceSink> {
    let db_source_or_sink = sqlx::query_as::<_, DbSourceSink>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_source_or_sink.transfer()
}

pub async fn read_sources_by_device_template_id(
    device_template_id: &String,
) -> Result<Vec<SourceSink>> {
    read_by_device_template_id(SourceSinkType::Source, device_template_id).await
}

pub async fn read_sinks_by_device_template_id(
    device_template_id: &String,
) -> Result<Vec<SourceSink>> {
    read_by_device_template_id(SourceSinkType::Sink, device_template_id).await
}

async fn read_by_device_template_id(
    source_sink_type: SourceSinkType,
    device_template_id: &String,
) -> Result<Vec<SourceSink>> {
    let db_sources_sinks = sqlx::query_as::<_, DbSourceSink>(
        format!(
            "SELECT * FROM {} WHERE source_sink_type = ? AND device_template_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(Into::<i32>::into(source_sink_type))
    .bind(device_template_id)
    .fetch_all(POOL.get().unwrap())
    .await?;

    db_sources_sinks
        .into_iter()
        .map(|db| db.transfer())
        .collect()
}

pub async fn read_sources_by_template_id(template_id: &String) -> Result<Vec<SourceSink>> {
    read_by_template_id(SourceSinkType::Source, template_id).await
}

pub async fn read_sinks_by_template_id(template_id: &String) -> Result<Vec<SourceSink>> {
    read_by_template_id(SourceSinkType::Sink, template_id).await
}

async fn read_by_template_id(
    source_sink_type: SourceSinkType,
    template_id: &String,
) -> Result<Vec<SourceSink>> {
    let db_sources_sinks = sqlx::query_as::<_, DbSourceSink>(
        format!(
            "SELECT * FROM {} WHERE source_sink_type = ? AND template_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(Into::<i32>::into(source_sink_type))
    .bind(template_id)
    .fetch_all(POOL.get().unwrap())
    .await?;
    db_sources_sinks
        .into_iter()
        .map(|db| db.transfer())
        .collect()
}

pub async fn search_sources(
    device_template_id: &String,
    pagination: Pagination,
    query: QueryParams,
) -> Result<(usize, Vec<SourceSink>)> {
    search(
        SourceSinkType::Source,
        device_template_id,
        pagination,
        query,
    )
    .await
}

pub async fn search_sinks(
    device_template_id: &String,
    pagination: Pagination,
    query: QueryParams,
) -> Result<(usize, Vec<SourceSink>)> {
    search(SourceSinkType::Sink, device_template_id, pagination, query).await
}

async fn search(
    source_sink_type: SourceSinkType,
    device_template_id: &String,
    pagination: Pagination,
    query: QueryParams,
) -> Result<(usize, Vec<SourceSink>)> {
    let source_sink_type: i32 = source_sink_type.into();
    let (limit, offset) = pagination.to_sql();
    let (count, db_sources_sinks) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE source_sink_type = ? AND device_templaet_id = ? AND name LIKE ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_sink_type)
            .bind(device_template_id)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, DbSourceSink>(
                format!("SELECT * FROM {} WHERE source_sink_type = ? AND device_template_id = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            )
            .bind(source_sink_type)
            .bind(device_template_id)
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap()).await?;

            (count, sources_or_sinks)
        }
        None => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE source_sink_type = ? AND device_template_id = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_sink_type)
            .bind(device_template_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, DbSourceSink>(
                format!("SELECT * FROM {} WHERE source_sink_type = ? AND device_template_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            )
            .bind(source_sink_type)
            .bind(device_template_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, sources_or_sinks)
        }
    };

    let sources_sinks = db_sources_sinks
        .into_iter()
        .map(|db| db.transfer())
        .collect::<Result<Vec<_>>>()?;

    Ok((count as usize, sources_sinks))
}

pub async fn count_sources_by_device_template_id(device_template_id: &String) -> Result<usize> {
    count_by_device_template_id(SourceSinkType::Source, device_template_id).await
}

pub async fn count_sinks_by_device_template_id(device_template_id: &String) -> Result<usize> {
    count_by_device_template_id(SourceSinkType::Sink, device_template_id).await
}

async fn count_by_device_template_id(
    source_sink_type: SourceSinkType,
    device_template_id: &String,
) -> Result<usize> {
    let source_sink_type: i32 = source_sink_type.into();
    let count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM {} WHERE source_sink_type = ? AND device_template_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(source_sink_type)
    .bind(device_template_id)
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

pub async fn update(id: &String, req: SourceSinkCreateUpdateReq) -> Result<()> {
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

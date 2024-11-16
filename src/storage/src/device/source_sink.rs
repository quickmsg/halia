use anyhow::Result;
use common::error::HaliaResult;
use sqlx::FromRow;
use tracing::debug;
use types::{
    devices::{device::source_sink::CreateUpdateReq, ConfType},
    Pagination, QuerySourcesOrSinksParams, Status,
};

use crate::{SourceSinkType, POOL};

static TABLE_NAME: &str = "device_sources_sinks";

#[derive(FromRow, Debug)]
struct DbSourceSink {
    pub id: String,
    pub device_id: String,
    pub device_template_source_sink_id: Option<String>,
    pub source_sink_type: i32,
    pub name: String,
    pub conf_type: i32,
    pub conf: Vec<u8>,
    pub template_id: Option<String>,
    pub status: i32,
    pub ts: i64,
}

impl DbSourceSink {
    pub fn transfer(self) -> Result<SourceSink> {
        debug!("conf: {:?}", self);
        Ok(SourceSink {
            id: self.id,
            device_id: self.device_id,
            device_template_source_sink_id: self.device_template_source_sink_id,
            source_sink_type: self.source_sink_type.try_into()?,
            name: self.name,
            conf_type: self.conf_type.try_into()?,
            conf: serde_json::from_slice(&self.conf)?,
            template_id: self.template_id,
            status: self.status.try_into()?,
            ts: self.ts,
        })
    }
}

pub struct SourceSink {
    pub id: String,
    pub device_id: String,
    pub device_template_source_sink_id: Option<String>,
    pub source_sink_type: SourceSinkType,
    pub name: String,
    pub conf_type: ConfType,
    pub conf: serde_json::Value,
    pub template_id: Option<String>,
    pub status: Status,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    device_id CHAR(32) NOT NULL,
    device_template_source_sink_id CHAR(32),
    source_sink_type SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    conf_type SMALLINT UNSIGNED NOT NULL,
    conf BLOB NOT NULL,
    template_id CHAR(32),
    status SMALLINT UNSIGNED NOT NULL,
    ts BIGINT UNSIGNED NOT NULL,
    UNIQUE (device_id, source_sink_type, name)
);
"#,
        TABLE_NAME
    )
}

pub async fn insert_source(
    id: &String,
    device_id: &String,
    device_template_source_id: Option<&String>,
    req: CreateUpdateReq,
) -> Result<()> {
    insert(
        SourceSinkType::Source,
        id,
        device_id,
        device_template_source_id,
        req,
    )
    .await
}

pub async fn insert_sink(
    id: &String,
    device_id: &String,
    device_template_sink_id: Option<&String>,
    req: CreateUpdateReq,
) -> Result<()> {
    insert(
        SourceSinkType::Sink,
        id,
        device_id,
        device_template_sink_id,
        req,
    )
    .await
}

async fn insert(
    source_sink_type: SourceSinkType,
    id: &String,
    device_id: &String,
    device_template_source_sink_id: Option<&String>,
    req: CreateUpdateReq,
) -> Result<()> {
    let source_sink_type: i32 = source_sink_type.into();
    let conf_type: i32 = req.conf_type.into();
    let conf = serde_json::to_vec(&req.conf)?;
    let ts = common::timestamp_millis() as i64;

    sqlx::query(
        format!(
            r#"INSERT INTO {} 
(id, device_id, device_template_source_sink_id, source_sink_type, name, conf_type, conf, template_id, err, ts) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(device_id)
    .bind(device_template_source_sink_id)
    .bind(source_sink_type)
    .bind(req.name)
    .bind(conf_type)
    .bind(conf)
    .bind(req.template_id)
    .bind(Into::<i32>::into(Status::default()))
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<SourceSink> {
    let source_or_sink = sqlx::query_as::<_, DbSourceSink>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    source_or_sink.transfer()
}

pub async fn read_id_by_device_template_source_sink_id(
    device_id: &String,
    device_template_source_sink_id: &String,
) -> Result<String> {
    let id: String = sqlx::query_scalar(
        format!(
            "SELECT id FROM {} WHERE device_id = ? AND device_template_source_sink_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(device_id)
    .bind(device_template_source_sink_id)
    .fetch_one(POOL.get().unwrap())
    .await?;
    Ok(id)
}

pub async fn read_many_ids_by_device_template_source_sink_id(
    device_template_source_sink_id: &String,
) -> Result<Vec<String>> {
    let ids: Vec<String> = sqlx::query_scalar(
        format!(
            "SELECT id FROM {} WHERE device_template_source_sink_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(device_template_source_sink_id)
    .fetch_all(POOL.get().unwrap())
    .await?;
    Ok(ids)
}

pub async fn read_sources_by_device_id(device_id: &String) -> Result<Vec<SourceSink>> {
    read_by_device_id(SourceSinkType::Source, device_id).await
}

pub async fn read_sinks_by_device_id(device_id: &String) -> Result<Vec<SourceSink>> {
    read_by_device_id(SourceSinkType::Sink, device_id).await
}

pub async fn update_status(id: &String, status: Status) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET status = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(Into::<i32>::into(status))
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

async fn read_by_device_id(
    source_sink_type: SourceSinkType,
    device_id: &String,
) -> Result<Vec<SourceSink>> {
    let db_sources_sinks = sqlx::query_as::<_, DbSourceSink>(
        format!(
            "SELECT * FROM {} WHERE source_sink_type = ? AND device_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(Into::<i32>::into(source_sink_type))
    .bind(device_id)
    .fetch_all(POOL.get().unwrap())
    .await?;

    let sources_sinks = db_sources_sinks
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<SourceSink>>>()?;
    Ok(sources_sinks)
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

    let sources_sinks = db_sources_sinks
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<SourceSink>>>()?;
    Ok(sources_sinks)
}

pub async fn search_sources(
    device_id: &String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> Result<(usize, Vec<SourceSink>)> {
    search(SourceSinkType::Source, device_id, pagination, query).await
}

pub async fn search_sinks(
    device_id: &String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> Result<(usize, Vec<SourceSink>)> {
    search(SourceSinkType::Sink, device_id, pagination, query).await
}

async fn search(
    source_sink_type: SourceSinkType,
    device_id: &String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> Result<(usize, Vec<SourceSink>)> {
    let source_sink_type: i32 = source_sink_type.into();
    let (limit, offset) = pagination.to_sql();
    let (count, db_sources_or_sinks) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE source_sink_type = ? AND device_id = ? AND name LIKE ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_sink_type)
            .bind(device_id)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, DbSourceSink>(
                format!("SELECT * FROM {} WHERE source_sink_type = ? AND device_id = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            )
            .bind(source_sink_type)
            .bind(device_id)
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap()).await?;

            (count, sources_or_sinks)
        }
        None => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE source_sink_type = ? AND device_id = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_sink_type)
            .bind(device_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, DbSourceSink>(
                format!("SELECT * FROM {} WHERE source_sink_type = ? AND device_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            )
            .bind(source_sink_type)
            .bind(device_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, sources_or_sinks)
        }
    };

    debug!("count: {}", count);

    let sources_or_sinks = db_sources_or_sinks
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<SourceSink>>>()?;

    debug!("here");
    Ok((count as usize, sources_or_sinks))
}

pub async fn count_sources_by_device_id(device_id: &String) -> Result<usize> {
    count_by_device_id(SourceSinkType::Source, device_id).await
}

pub async fn count_sinks_by_device_id(device_id: &String) -> Result<usize> {
    count_by_device_id(SourceSinkType::Sink, device_id).await
}

async fn count_by_device_id(source_sink_type: SourceSinkType, device_id: &String) -> Result<usize> {
    let source_sink_type: i32 = source_sink_type.into();
    let count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM {} WHERE source_sink_type = ? AND device_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(source_sink_type)
    .bind(device_id)
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

pub async fn update(id: &String, req: CreateUpdateReq) -> Result<()> {
    let conf_type: i32 = req.conf_type.into();
    let conf = serde_json::to_vec(&req.conf)?;
    sqlx::query(
        format!(
            "UPDATE {} SET name = ?, conf_type = ?, conf = ?, template_id = ? WHERE id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(req.name)
    .bind(conf_type)
    .bind(conf)
    .bind(req.template_id)
    .bind(id)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn delete_many_by_device_id(device_id: &String) -> Result<()> {
    sqlx::query(format!("DELETE FROM {} WHERE device_id = ?", TABLE_NAME).as_str())
        .bind(device_id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn check_exists(id: &String) -> Result<bool> {
    let count: i64 =
        sqlx::query_scalar(format!("SELECT COUNT(*) FROM {} WHERE id = ?", TABLE_NAME).as_str())
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

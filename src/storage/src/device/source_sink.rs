use anyhow::Result;
use common::error::HaliaResult;
use sqlx::FromRow;
use types::{
    devices::{Metadatas, SourceFromType, SourceSinkCreateUpdateReq, SourceSinkQueryParams},
    Pagination, SourceSinkType, Status,
};

use crate::POOL;

static TABLE_NAME: &str = "device_sources_sinks";

#[derive(FromRow, Debug)]
struct DbSourceSink {
    pub id: String,
    pub device_id: String,
    pub source_from_type: i32,
    pub device_template_source_sink_id: Option<String>,
    pub source_group_source_id: Option<String>,
    pub device_source_group_id: Option<String>,
    // pub from_id: Option<String>,
    pub name: String,
    pub conf: Vec<u8>,
    pub status: i32,
    pub ts: i64,
}

impl DbSourceSink {
    pub fn transfer(self) -> Result<SourceSink> {
        Ok(SourceSink {
            id: self.id,
            device_id: self.device_id,
            source_from_type: self.source_from_type.try_into()?,
            device_template_source_sink_id: self.device_template_source_sink_id,
            source_group_source_id: self.source_group_source_id,
            device_source_group_id: self.device_source_group_id,
            name: self.name,
            conf: serde_json::from_slice(&self.conf)?,
            status: self.status.try_into()?,
            ts: self.ts,
        })
    }
}

#[derive(Debug)]
pub struct SourceSink {
    pub id: String,
    pub device_id: String,
    pub source_from_type: SourceFromType,
    pub device_template_source_sink_id: Option<String>,
    pub source_group_source_id: Option<String>,
    pub device_source_group_id: Option<String>,
    pub name: String,
    pub conf: serde_json::Value,
    pub status: Status,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    device_id CHAR(32) NOT NULL,
    source_from_type SMALLINT UNSIGNED NOT NULL,
    device_template_source_sink_id CHAR(32),
    source_group_source_id CHAR(32),
    device_source_group_id CHAR(32),
    source_sink_type SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    conf BLOB,
    status SMALLINT UNSIGNED NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn device_insert_source(
    source_id: &String,
    status: Status,
    device_id: &String,
    req: SourceSinkCreateUpdateReq,
) -> Result<()> {
    device_insert(source_id, status, device_id, req, SourceSinkType::Source).await
}

pub async fn device_insert_sink(
    sink_id: &String,
    status: Status,
    device_id: &String,
    req: SourceSinkCreateUpdateReq,
) -> Result<()> {
    device_insert(sink_id, status, device_id, req, SourceSinkType::Sink).await
}

async fn device_insert(
    id: &String,
    stauts: Status,
    device_id: &String,
    req: SourceSinkCreateUpdateReq,
    source_sink_type: SourceSinkType,
) -> Result<()> {
    sqlx::query(
        format!(
            r#"INSERT INTO {} 
(id, device_id, source_from_type, source_sink_type, name, conf, status, ts) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(device_id)
    .bind(Into::<i32>::into(SourceFromType::Device))
    .bind(Into::<i32>::into(source_sink_type))
    .bind(req.name)
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(Into::<i32>::into(stauts))
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn device_template_insert_source(
    source_id: &String,
    status: Status,
    device_id: &String,
    name: &String,
    device_template_source_id: &String,
) -> Result<()> {
    device_template_insert(
        source_id,
        status,
        device_id,
        name,
        device_template_source_id,
        SourceSinkType::Source,
    )
    .await
}

pub async fn device_template_insert_sink(
    id: &String,
    status: Status,
    device_id: &String,
    name: &String,
    device_template_source_id: &String,
) -> Result<()> {
    device_template_insert(
        id,
        status,
        device_id,
        name,
        device_template_source_id,
        SourceSinkType::Sink,
    )
    .await
}

async fn device_template_insert(
    id: &String,
    status: Status,
    device_id: &String,
    name: &String,
    device_template_source_sink_id: &String,
    source_sink_type: SourceSinkType,
) -> Result<()> {
    sqlx::query(
        format!(
            r#"INSERT INTO {}
(id, device_id, source_from_type, device_template_source_sink_id, source_sink_type, name, conf, status, ts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(device_id)
    .bind(Into::<i32>::into(SourceFromType::DeviceTemplate))
    .bind(device_template_source_sink_id)
    .bind(Into::<i32>::into(source_sink_type))
    .bind(name)
    .bind(serde_json::to_vec(&Metadatas::default())?)
    .bind(Into::<i32>::into(status))
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn source_group_insert_source(
    id: &String,
    status: Status,
    device_id: &String,
    name: &String,
    source_group_source_id: &String,
    device_source_group_id: &String,
) -> Result<()> {
    sqlx::query(
        format!(
            r#"INSERT INTO {}
(id, device_id, source_from_type, source_group_source_id, device_source_group_id, source_sink_type, name, conf, status, ts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(device_id)
    .bind(Into::<i32>::into(SourceFromType::SourceGroup))
    .bind(source_group_source_id)
    .bind(device_source_group_id)
    .bind(Into::<i32>::into(SourceSinkType::Source))
    .bind(name)
    .bind(serde_json::to_vec(&Metadatas::default())?)
    .bind(Into::<i32>::into(status))
    .bind(common::timestamp_millis() as i64)
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

pub async fn update_status_by_device_id(device_id: &String, status: Status) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET status = ? WHERE device_id = ?", TABLE_NAME).as_str())
        .bind(Into::<i32>::into(status))
        .bind(device_id)
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

pub async fn read_sources_by_device_template_source_id(
    device_template_source_id: &String,
) -> Result<Vec<SourceSink>> {
    read_by_device_template_source_sink_id(SourceSinkType::Source, device_template_source_id).await
}

pub async fn read_sinks_by_device_template_sink_id(
    device_template_sink_id: &String,
) -> Result<Vec<SourceSink>> {
    read_by_device_template_source_sink_id(SourceSinkType::Sink, device_template_sink_id).await
}

async fn read_by_device_template_source_sink_id(
    source_sink_type: SourceSinkType,
    device_template_source_sink_id: &String,
) -> Result<Vec<SourceSink>> {
    let db_sources_sinks = sqlx::query_as::<_, DbSourceSink>(
        format!(
            "SELECT * FROM {} WHERE source_sink_type = ? AND device_template_source_sink_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(Into::<i32>::into(source_sink_type))
    .bind(device_template_source_sink_id)
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
    query: SourceSinkQueryParams,
) -> Result<(usize, Vec<SourceSink>)> {
    search(SourceSinkType::Source, device_id, pagination, query).await
}

pub async fn search_sinks(
    device_id: &String,
    pagination: Pagination,
    query: SourceSinkQueryParams,
) -> Result<(usize, Vec<SourceSink>)> {
    search(SourceSinkType::Sink, device_id, pagination, query).await
}

async fn search(
    source_sink_type: SourceSinkType,
    device_id: &String,
    pagination: Pagination,
    query: SourceSinkQueryParams,
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

    let sources_or_sinks = db_sources_or_sinks
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<SourceSink>>>()?;

    Ok((count as usize, sources_or_sinks))
}

pub async fn search_by_template_id(
    template_id: &String,
    pagination: Pagination,
) -> Result<(usize, Vec<SourceSink>)> {
    let (limit, offset) = pagination.to_sql();

    let count: i64 = sqlx::query_scalar(
        format!("SELECT COUNT(*) FROM {} WHERE template_id = ?", TABLE_NAME).as_str(),
    )
    .bind(template_id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    let db_sources_or_sinks = sqlx::query_as::<_, DbSourceSink>(
        format!(
            "SELECT * FROM {} WHERE template_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(template_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(POOL.get().unwrap())
    .await?;

    let sources_or_sinks = db_sources_or_sinks
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<SourceSink>>>()?;

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

pub async fn update(id: &String, req: SourceSinkCreateUpdateReq) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET name = ?, conf = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(serde_json::to_vec(&req.conf)?)
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

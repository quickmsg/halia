// 设备模板下的源和动作
use anyhow::Result;
use common::error::HaliaResult;
use sqlx::FromRow;
use types::{
    devices::source_sink_template::CreateUpdateReq, Pagination, QuerySourcesOrSinksParams,
};

use crate::{SourceSinkType, POOL};

static TABLE_NAME: &str = "device_template_sources_sinks";

#[derive(FromRow)]
pub struct SourceSink {
    pub id: String,
    pub device_tempalate_id: String,
    pub typ: i32,
    pub name: String,
    pub des: Option<Vec<u8>>,
    pub conf_type: i32,
    pub template_id: Option<String>,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    device_template_id CHAR(32) NOT NULL,
    typ SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    des BLOB,
    conf_type SMALLINT UNSIGNED NOT NULL,
    template_id CHAR(32),
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL,
    UNIQUE (device_id, typ, name)
);
"#,
        TABLE_NAME
    )
}

pub async fn insert_source(id: &String, device_id: &String, req: CreateUpdateReq) -> Result<()> {
    insert(SourceSinkType::Source, id, device_id, req).await
}

pub async fn insert_sink(id: &String, device_id: &String, req: CreateUpdateReq) -> Result<()> {
    insert(SourceSinkType::Sink, id, device_id, req).await
}

async fn insert(
    typ: SourceSinkType,
    id: &String,
    device_id: &String,
    req: CreateUpdateReq,
) -> Result<()> {
    let typ: i32 = typ.into();
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf_type: i32 = req.conf_type.into();
    let conf = serde_json::to_vec(&req.conf)?;
    let ts = common::timestamp_millis();

    sqlx::query(
        format!(
            "INSERT INTO {} (id, device_id, typ, name, des, conf_type, template_id, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(device_id)
    .bind(typ)
    .bind(req.base.name)
    .bind(desc)
    .bind(conf_type)
    .bind(req.template_id)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<SourceSink> {
    let source_or_sink = sqlx::query_as::<_, SourceSink>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;
    Ok(source_or_sink)
}

pub async fn read_sources_by_device_id(device_id: &String) -> Result<Vec<SourceSink>> {
    read_by_device_id(SourceSinkType::Source, device_id).await
}

pub async fn read_sinks_by_device_id(device_id: &String) -> Result<Vec<SourceSink>> {
    read_by_device_id(SourceSinkType::Sink, device_id).await
}

async fn read_by_device_id(typ: SourceSinkType, device_id: &String) -> Result<Vec<SourceSink>> {
    let typ: i32 = typ.into();
    let sources_sinks = sqlx::query_as::<_, SourceSink>(
        format!(
            "SELECT * FROM {} WHERE typ = ? AND device_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(typ)
    .bind(device_id)
    .fetch_all(POOL.get().unwrap())
    .await?;
    Ok(sources_sinks)
}

pub async fn read_sources_by_template_id(template_id: &String) -> Result<Vec<SourceSink>> {
    read_by_template_id(SourceSinkType::Source, template_id).await
}

pub async fn read_sinks_by_template_id(template_id: &String) -> Result<Vec<SourceSink>> {
    read_by_template_id(SourceSinkType::Sink, template_id).await
}

async fn read_by_template_id(typ: SourceSinkType, template_id: &String) -> Result<Vec<SourceSink>> {
    let typ: i32 = typ.into();
    let sources_sinks = sqlx::query_as::<_, SourceSink>(
        format!(
            "SELECT * FROM {} WHERE typ = ? AND template_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(typ)
    .bind(template_id)
    .fetch_all(POOL.get().unwrap())
    .await?;
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
    typ: SourceSinkType,
    device_id: &String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> Result<(usize, Vec<SourceSink>)> {
    let typ: i32 = typ.into();
    let (limit, offset) = pagination.to_sql();
    let (count, sources_or_sinks) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE typ = ? AND device_id = ? AND name LIKE ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(typ)
            .bind(device_id)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, SourceSink>(
                format!("SELECT * FROM {} WHERE typ = ? AND device_id = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            )
            .bind(typ)
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
                    "SELECT COUNT(*) FROM {} WHERE typ = ? AND device_id = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(typ)
            .bind(device_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, SourceSink>(
                format!("SELECT * FROM {} WHERE typ = ? AND device_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            )
            .bind(typ)
            .bind(device_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, sources_or_sinks)
        }
    };

    Ok((count as usize, sources_or_sinks))
}

pub async fn count_sources_by_device_id(device_id: &String) -> Result<usize> {
    count_by_device_id(SourceSinkType::Source, device_id).await
}

pub async fn count_sinks_by_device_id(device_id: &String) -> Result<usize> {
    count_by_device_id(SourceSinkType::Sink, device_id).await
}

async fn count_by_device_id(typ: SourceSinkType, device_id: &String) -> Result<usize> {
    let typ: i32 = typ.into();
    let count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM {} WHERE typ = ? AND device_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(typ)
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
    let conf = serde_json::to_vec(&req.conf)?;
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    sqlx::query(
        format!(
            "UPDATE {} SET name = ?, des = ?, conf = ? WHERE id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(req.base.name)
    .bind(desc)
    .bind(conf)
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

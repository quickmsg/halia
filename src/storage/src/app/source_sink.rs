use anyhow::Result;
use common::error::HaliaResult;
use sqlx::FromRow;
use types::{
    apps::{CreateUpdateSourceSinkReq, QuerySourcesSinksParams},
    Pagination, Status,
};

use crate::SourceSinkType;

use super::POOL;

static TABLE_NAME: &str = "app_sources_sinks";

#[derive(FromRow)]
struct DbSourceSink {
    pub id: String,
    pub app_id: String,
    pub name: String,
    pub conf: Vec<u8>,
    pub status: i32,
    pub ts: i64,
}

impl DbSourceSink {
    pub fn transfer(self) -> Result<SourceSink> {
        Ok(SourceSink {
            id: self.id,
            app_id: self.app_id,
            name: self.name,
            conf: serde_json::from_slice(&self.conf)?,
            status: self.status.try_into()?,
            ts: self.ts,
        })
    }
}

pub struct SourceSink {
    pub id: String,
    pub app_id: String,
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
    source_sink_type SMALLINT UNSIGNED NOT NULL,
    app_id CHAR(32) NOT NULL,
    name VARCHAR(255) NOT NULL,
    conf BLOB NOT NULL,
    status SMALLINT UNSIGNED NOT NULL,
    ts BIGINT UNSIGNED NOT NULL,
    UNIQUE (app_id, source_sink_type, name)
);
"#,
        TABLE_NAME
    )
}

pub async fn insert_source(
    app_id: &String,
    id: &String,
    req: CreateUpdateSourceSinkReq,
) -> Result<()> {
    insert(SourceSinkType::Source, app_id, id, req).await
}

pub async fn insert_sink(
    app_id: &String,
    id: &String,
    req: CreateUpdateSourceSinkReq,
) -> Result<()> {
    insert(SourceSinkType::Sink, app_id, id, req).await
}

async fn insert(
    source_sink_type: SourceSinkType,
    app_id: &String,
    id: &String,
    req: CreateUpdateSourceSinkReq,
) -> Result<()> {
    sqlx::query(
        format!(
            "INSERT INTO {} (id, source_sink_type, app_id, name, conf, status, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(Into::<i32>::into(source_sink_type))
    .bind(app_id)
    .bind(req.name)
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(Into::<i32>::into(Status::default()))
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_all_sources_by_app_id(app_id: &String) -> Result<Vec<SourceSink>> {
    read_all_by_app_id(SourceSinkType::Source, app_id).await
}

pub async fn read_all_sinks_by_app_id(app_id: &String) -> Result<Vec<SourceSink>> {
    read_all_by_app_id(SourceSinkType::Sink, app_id).await
}

async fn read_all_by_app_id(
    source_sink_type: SourceSinkType,
    app_id: &String,
) -> Result<Vec<SourceSink>> {
    let db_sources_sinks = sqlx::query_as::<_, DbSourceSink>(
        format!(
            "SELECT * FROM {} WHERE app_id = ? AND source_sink_type = ? ORDER BY ts DESC",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(app_id)
    .bind(Into::<i32>::into(source_sink_type))
    .fetch_all(POOL.get().unwrap())
    .await?;

    let sources_sinks = db_sources_sinks
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<SourceSink>>>()?;

    Ok(sources_sinks)
}

pub async fn read_one(id: &String) -> Result<SourceSink> {
    let db_source_sink = sqlx::query_as::<_, DbSourceSink>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;
    let source_sink = db_source_sink.transfer()?;
    Ok(source_sink)
}

pub async fn query_sources_by_app_id(
    app_id: &String,
    pagination: Pagination,
    query: QuerySourcesSinksParams,
) -> Result<(usize, Vec<SourceSink>)> {
    query_by_app_id(SourceSinkType::Source, app_id, pagination, query).await
}

pub async fn query_sinks_by_app_id(
    app_id: &String,
    pagination: Pagination,
    query: QuerySourcesSinksParams,
) -> Result<(usize, Vec<SourceSink>)> {
    query_by_app_id(SourceSinkType::Sink, app_id, pagination, query).await
}

async fn query_by_app_id(
    source_sink_type: SourceSinkType,
    app_id: &String,
    pagination: Pagination,
    query: QuerySourcesSinksParams,
) -> Result<(usize, Vec<SourceSink>)> {
    let source_sink_type: i32 = source_sink_type.into();
    let (limit, offset) = pagination.to_sql();
    let (count, db_sources_sinks) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE app_id = ? AND source_sink_type = ? AND name LIKE ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(app_id)
            .bind(source_sink_type)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let db_sources_sinks = sqlx::query_as::<_, DbSourceSink>(
                format!("SELECT * FROM {} WHERE app_id = ? AND source_sink_type = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            ).bind(app_id)
            .bind(source_sink_type)
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap()).await?;

            (count, db_sources_sinks)
        }
        None => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE app_id = ? AND source_sink_type = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(app_id)
            .bind(source_sink_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let db_sources_sinks = sqlx::query_as::<_, DbSourceSink>(
                format!("SELECT * FROM {} WHERE app_id = ? AND source_sink_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            )
            .bind(app_id)
            .bind(source_sink_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, db_sources_sinks)
        }
    };

    let sources_sinks = db_sources_sinks
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<SourceSink>>>()?;

    Ok((count as usize, sources_sinks))
}

pub async fn count_sources_by_app_id(app_id: &String) -> Result<usize> {
    count_by_app_id(SourceSinkType::Source, app_id).await
}

pub async fn count_sinks_by_app_id(app_id: &String) -> Result<usize> {
    count_by_app_id(SourceSinkType::Sink, app_id).await
}

async fn count_by_app_id(source_sink_type: SourceSinkType, app_id: &String) -> Result<usize> {
    let count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM {} WHERE app_id = ? AND source_sink_type = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(app_id)
    .bind(Into::<i32>::into(source_sink_type))
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

pub async fn update(id: &String, req: CreateUpdateSourceSinkReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.conf)?;
    sqlx::query(format!("UPDATE {} SET name = ?, conf = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete_by_app_id(app_id: &String) -> Result<()> {
    sqlx::query(format!("DELETE FROM {} WHERE app_id = ?", TABLE_NAME).as_str())
        .bind(app_id)
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

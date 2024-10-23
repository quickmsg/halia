use anyhow::Result;
use common::error::HaliaResult;
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    devices::source_sink_template::{CreateReq, QueryParams, UpdateReq},
    Pagination,
};

use crate::SourceSinkType;

use crate::POOL;

const TABLE_NAME: &str = "device_source_sink_templates";

#[derive(FromRow)]
pub struct SourceSinkTemplate {
    pub id: String,
    pub source_sink_type: i32,
    pub device_type: i32,
    pub name: String,
    pub des: Option<Vec<u8>>,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    source_sink_type SMALLINT UNSIGNED NOT NULL,
    device_type SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(
    id: &String,
    source_sink_type: SourceSinkType,
    req: CreateReq,
) -> HaliaResult<()> {
    let source_sink_type: i32 = source_sink_type.into();
    let device_type: i32 = req.device_type.into();
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf = serde_json::to_vec(&req.conf)?;
    let ts = common::timestamp_millis();
    sqlx::query(
        format!(
            r#"INSERT INTO {} 
(id, source_sink_type, device_type, name, des, conf, ts) 
VALUES (?, ?, ?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(source_sink_type)
    .bind(device_type)
    .bind(req.base.name)
    .bind(desc)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_conf(id: &String) -> Result<Vec<u8>> {
    let conf: Vec<u8> =
        sqlx::query_scalar(format!("SELECT conf FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(conf)
}

pub async fn search_source_templates(
    pagination: Pagination,
    query: QueryParams,
) -> Result<(usize, Vec<SourceSinkTemplate>)> {
    search(pagination, SourceSinkType::Source, query).await
}

pub async fn search_sink_templates(
    pagination: Pagination,
    query: QueryParams,
) -> Result<(usize, Vec<SourceSinkTemplate>)> {
    search(pagination, SourceSinkType::Sink, query).await
}

async fn search(
    pagination: Pagination,
    source_sink_type: SourceSinkType,
    query: QueryParams,
) -> Result<(usize, Vec<SourceSinkTemplate>)> {
    let (limit, offset) = pagination.to_sql();
    let source_sink_type: i32 = source_sink_type.into();
    let (count, templates) = match (&query.name, &query.device_type) {
        (None, None) => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE source_sink_type = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_sink_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let devices = sqlx::query_as::<_, SourceSinkTemplate>(
                format!(
                    "SELECT * FROM {} WHERE source_sink_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(source_sink_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, devices)
        }
        _ => {
            let mut where_clause = String::from("WHERE source_sink_type = ?");

            if query.name.is_some() {
                where_clause.push_str(" AND name LIKE ?");
            }
            if query.device_type.is_some() {
                where_clause.push_str(" AND device_type = ?");
            }

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(&query_count_str);

            let query_schemas_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );
            let mut query_schemas_builder: QueryAs<'_, Any, SourceSinkTemplate, AnyArguments> =
                sqlx::query_as::<_, SourceSinkTemplate>(&query_schemas_str);

            query_count_builder = query_count_builder.bind(source_sink_type);
            query_schemas_builder = query_schemas_builder.bind(source_sink_type);

            if let Some(name) = query.name {
                let name = format!("%{}%", name);
                query_count_builder = query_count_builder.bind(name.clone());
                query_schemas_builder = query_schemas_builder.bind(name);
            }
            if let Some(device_type) = query.device_type {
                let device_type: i32 = device_type.into();
                query_count_builder = query_count_builder.bind(device_type);
                query_schemas_builder = query_schemas_builder.bind(device_type);
            }

            let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
            let templates = query_schemas_builder
                .bind(limit)
                .bind(offset)
                .fetch_all(POOL.get().unwrap())
                .await?;

            (count, templates)
        }
    };

    Ok((count as usize, templates))
}

pub async fn read_name(id: &String) -> Result<String> {
    let name: String = sqlx::query_scalar("SELECT name FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(name)
}

pub async fn update(id: &String, req: UpdateReq) -> HaliaResult<()> {
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf = serde_json::to_vec(&req.conf)?;
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

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    crate::delete_by_id(id, TABLE_NAME).await
}

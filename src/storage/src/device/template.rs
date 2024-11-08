/// 设备模板
use anyhow::Result;
use common::error::HaliaResult;
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    devices::device_template::{CreateReq, QueryParams, UpdateReq},
    Pagination,
};

use crate::POOL;

use super::template_source_sink;

const TABLE_NAME: &str = "device_templates";

#[derive(FromRow)]
pub struct DeviceTemplate {
    pub id: String,
    pub device_type: i32,
    pub name: String,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    device_type SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateReq) -> HaliaResult<()> {
    let conf = serde_json::to_vec(&req.conf)?;
    let ts = common::timestamp_millis() as i64;
    let device_type: i32 = req.device_type.into();
    sqlx::query(
        format!(
            "INSERT INTO {} (id, device_type, name, conf, ts) VALUES (?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(device_type)
    .bind(req.name)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<DeviceTemplate> {
    let device = sqlx::query_as::<_, DeviceTemplate>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    Ok(device)
}

pub async fn read_conf(id: &String) -> Result<Vec<u8>> {
    let conf: Vec<u8> =
        sqlx::query_scalar(format!("SELECT conf FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(conf)
}

pub async fn check_exists(id: &String) -> Result<bool> {
    let count: i64 =
        sqlx::query_scalar(format!("SELECT COUNT(*) FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(count > 0)
}

pub async fn read_device_type(id: &String) -> Result<i32> {
    let device_type: i32 =
        sqlx::query_scalar(format!("SELECT device_type FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(device_type)
}

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<DeviceTemplate>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, devices) = match (&query_params.name, &query_params.device_type) {
        (None, None) => {
            let count: i64 =
                sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let devices = sqlx::query_as::<_, DeviceTemplate>(
                format!(
                    "SELECT * FROM {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, devices)
        }
        _ => {
            let mut where_clause = String::new();

            if query_params.name.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE name LIKE ?"),
                    false => where_clause.push_str(" AND name LIKE ?"),
                }
            }
            if query_params.device_type.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE device_type = ?"),
                    false => where_clause.push_str(" AND device_type = ?"),
                }
            }

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(&query_count_str);

            let query_schemas_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );
            let mut query_schemas_builder: QueryAs<'_, Any, DeviceTemplate, AnyArguments> =
                sqlx::query_as::<_, DeviceTemplate>(&query_schemas_str);

            if let Some(name) = query_params.name {
                let name = format!("%{}%", name);
                query_count_builder = query_count_builder.bind(name.clone());
                query_schemas_builder = query_schemas_builder.bind(name);
            }
            if let Some(device_type) = query_params.device_type {
                let device_type: i32 = device_type.into();
                query_count_builder = query_count_builder.bind(device_type);
                query_schemas_builder = query_schemas_builder.bind(device_type);
            }

            let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
            let devices = query_schemas_builder
                .bind(limit)
                .bind(offset)
                .fetch_all(POOL.get().unwrap())
                .await?;

            (count, devices)
        }
    };

    Ok((count as usize, devices))
}

pub async fn update(id: &String, req: UpdateReq) -> HaliaResult<()> {
    let conf = serde_json::to_vec(&req.conf)?;
    sqlx::query(format!("UPDATE {} SET name = ?, conf = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    crate::delete_by_id(id, TABLE_NAME).await?;
    template_source_sink::delete_many_by_device_template_id(id).await
}
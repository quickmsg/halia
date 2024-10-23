use anyhow::Result;
use common::error::HaliaResult;
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    devices::device::{CreateReq, QueryParams, UpdateReq},
    Pagination,
};

use crate::POOL;

const TABLE_NAME: &str = "devices";

#[derive(FromRow)]
pub struct Device {
    pub id: String,
    pub protocol: i32,
    pub name: String,
    pub des: Option<Vec<u8>>,
    pub conf_type: i32,
    pub conf: Vec<u8>,
    pub template_id: Option<String>,
    pub status: i32, // 0:close 1:open
    pub err: i32,    // 0:错误中 1:正常中
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    protocol SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    des BLOB,
    conf_type SMALLINT UNSIGNED NOT NULL,
    conf BLOB NOT NULL,
    template_id CHAR(32),
    status SMALLINT UNSIGNED NOT NULL,
    err SMALLINT UNSIGNED NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateReq) -> HaliaResult<()> {
    let protocol: i32 = req.protocol.into();
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf_type: i32 = req.conf_type.into();
    let conf = serde_json::to_vec(&req.conf)?;
    let ts = common::timestamp_millis();
    sqlx::query(
        format!(
            r#"INSERT INTO {} 
(id, protocol, name, des, conf_type, conf, template_id, status, err, ts) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(protocol)
    .bind(req.base.name)
    .bind(desc)
    .bind(conf_type)
    .bind(conf)
    .bind(req.template_id)
    .bind(false as i32)
    .bind(false as i32)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<Device> {
    let device =
        sqlx::query_as::<_, Device>(format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str())
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

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Device>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, devices) = match (
        &query_params.name,
        &query_params.protocol,
        &query_params.on,
        &query_params.err,
    ) {
        (None, None, None, None) => {
            let count: i64 =
                sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let devices = sqlx::query_as::<_, Device>(
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
            if query_params.protocol.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE protocol = ?"),
                    false => where_clause.push_str(" AND protocol = ?"),
                }
            }
            if query_params.on.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE status = ?"),
                    false => where_clause.push_str(" AND status = ?"),
                }
            }
            if query_params.err.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE err = ?"),
                    false => where_clause.push_str(" AND err = ?"),
                }
            }

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(&query_count_str);

            let query_schemas_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );
            let mut query_schemas_builder: QueryAs<'_, Any, Device, AnyArguments> =
                sqlx::query_as::<_, Device>(&query_schemas_str);

            if let Some(name) = query_params.name {
                let name = format!("%{}%", name);
                query_count_builder = query_count_builder.bind(name.clone());
                query_schemas_builder = query_schemas_builder.bind(name);
            }
            if let Some(protocol) = query_params.protocol {
                let protocol: i32 = protocol.into();
                query_count_builder = query_count_builder.bind(protocol);
                query_schemas_builder = query_schemas_builder.bind(protocol);
            }
            if let Some(on) = query_params.on {
                query_count_builder = query_count_builder.bind(on as i32);
                query_schemas_builder = query_schemas_builder.bind(on as i32);
            }
            if let Some(err) = query_params.err {
                query_count_builder = query_count_builder.bind(err as i32);
                query_schemas_builder = query_schemas_builder.bind(err as i32);
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

pub async fn read_many_on() -> Result<Vec<Device>> {
    let devices = sqlx::query_as::<_, Device>(
        format!("SELECT * FROM {} WHERE status = 1", TABLE_NAME).as_str(),
    )
    .fetch_all(POOL.get().unwrap())
    .await?;

    Ok(devices)
}

pub async fn read_name(id: &String) -> Result<String> {
    let name: String =
        sqlx::query_scalar(format!("SELECT name FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(name)
}

pub async fn read_protocol(id: &String) -> Result<i32> {
    let typ: i32 =
        sqlx::query_scalar(format!("SELECT protocol FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(typ)
}

pub async fn count_all() -> Result<usize> {
    let count: i64 = sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

pub async fn update_status(id: &String, status: bool) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET status = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(status as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn update_err(id: &String, err: bool) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET err = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(err as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn update_conf(id: &String, req: UpdateReq) -> HaliaResult<()> {
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf_type: i32 = req.conf_type.into();
    let conf = serde_json::to_vec(&req.conf)?;
    sqlx::query(
        format!(
            "UPDATE {} SET name = ?, des = ?, conf_type = ?, conf = ?, template_id = ? WHERE id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(req.base.name)
    .bind(desc)
    .bind(conf_type)
    .bind(conf)
    .bind(req.template_id)
    .bind(id)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    crate::delete_by_id(id, TABLE_NAME).await
}

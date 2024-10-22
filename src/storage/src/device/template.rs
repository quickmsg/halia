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
    devices::{
        device_template::{CreateUpdateReq, QueryParams},
        CreateUpdateDeviceReq,
    },
    Pagination,
};

use super::POOL;

const TABLE_NAME: &str = "device_templates";

#[derive(FromRow)]
pub struct DeviceTemplate {
    pub id: String,
    pub typ: i32,
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
    typ SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateUpdateReq) -> HaliaResult<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = common::timestamp_millis();
    let typ: i32 = req.typ.into();
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    sqlx::query(
        format!(
            "INSERT INTO {} (id, typ, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(typ)
    .bind(req.base.name)
    .bind(desc)
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

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<DeviceTemplate>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, devices) = match (&query_params.name, &query_params.typ) {
        (None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
                .fetch_one(POOL.get().unwrap())
                .await?;

            let devices = sqlx::query_as::<_, DeviceTemplate>(
                "SELECT * FROM devices ORDER BY ts DESC LIMIT ? OFFSET ?",
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
            if query_params.typ.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE typ = ?"),
                    false => where_clause.push_str(" AND typ = ?"),
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
            if let Some(typ) = query_params.typ {
                let typ: i32 = typ.into();
                query_count_builder = query_count_builder.bind(typ);
                query_schemas_builder = query_schemas_builder.bind(typ);
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

pub async fn read_on() -> Result<Vec<DeviceTemplate>> {
    let devices = sqlx::query_as::<_, DeviceTemplate>("SELECT * FROM devices WHERE status = 1")
        .fetch_all(POOL.get().unwrap())
        .await?;

    Ok(devices)
}

pub async fn read_type(id: &String) -> Result<i32> {
    let typ: i32 = sqlx::query_scalar("SELECT typ FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(typ)
}

pub async fn count_all() -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

pub async fn update_conf(id: &String, req: CreateUpdateDeviceReq) -> HaliaResult<()> {
    let conf = serde_json::to_vec(&req.conf.ext)?;
    let desc = req.conf.base.desc.map(|desc| desc.into_bytes());
    sqlx::query("UPDATE devices SET name = ?, des = ?, conf = ? WHERE id = ?")
        .bind(req.conf.base.name)
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

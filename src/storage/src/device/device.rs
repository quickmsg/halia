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
        device::{CreateReq, QueryParams, UpdateReq},
        ConfType, DeviceType,
    },
    Pagination, Status,
};

use crate::POOL;

const TABLE_NAME: &str = "devices";

#[derive(FromRow)]
struct DbDevice {
    pub id: String,
    pub device_type: i32,
    pub name: String,
    pub conf_type: i32,
    pub conf: Vec<u8>,
    pub template_id: Option<String>,
    pub status: i32,
    pub ts: i64,
}

impl DbDevice {
    pub fn transfer(self) -> Result<Device> {
        Ok(Device {
            id: self.id,
            device_type: self.device_type.try_into()?,
            name: self.name,
            conf_type: self.conf_type.try_into()?,
            conf: serde_json::from_slice(&self.conf)?,
            template_id: self.template_id,
            status: self.status.try_into()?,
            ts: self.ts,
        })
    }
}

pub struct Device {
    pub id: String,
    pub device_type: DeviceType,
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
    device_type SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    conf_type SMALLINT UNSIGNED NOT NULL,
    conf BLOB NOT NULL,
    template_id CHAR(32),
    status SMALLINT UNSIGNED NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateReq) -> HaliaResult<()> {
    sqlx::query(
        format!(
            r#"INSERT INTO {} 
(id, device_type, name, conf_type, conf, template_id, status, ts) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(Into::<i32>::into(req.device_type))
    .bind(req.name)
    .bind(Into::<i32>::into(req.conf_type))
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(req.template_id)
    .bind(Into::<i32>::into(Status::Stopped))
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<Device> {
    let db_device = sqlx::query_as::<_, DbDevice>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_device.transfer()
}

pub async fn read_conf(id: &String) -> Result<serde_json::Value> {
    let conf: Vec<u8> =
        sqlx::query_scalar(format!("SELECT conf FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    let conf = serde_json::from_slice::<serde_json::Value>(&conf)?;
    Ok(conf)
}

pub async fn search(pagination: Pagination, query: QueryParams) -> Result<(usize, Vec<Device>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, db_devices) = match (&query.name, &query.device_type, &query.status) {
        (None, None, None) => {
            let count: i64 =
                sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let devices = sqlx::query_as::<_, DbDevice>(
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

            if query.name.is_some() {
                where_clause.push_str("WHERE name LIKE ?")
            }
            if query.device_type.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE device_type = ?"),
                    false => where_clause.push_str(" AND device_type = ?"),
                }
            }
            if query.status.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE status = ?"),
                    false => where_clause.push_str(" AND status = ?"),
                }
            }

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(&query_count_str);

            let query_schemas_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );
            let mut query_schemas_builder: QueryAs<'_, Any, DbDevice, AnyArguments> =
                sqlx::query_as::<_, DbDevice>(&query_schemas_str);

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
            if let Some(status) = query.status {
                let status: i32 = status.into();
                query_count_builder = query_count_builder.bind(status);
                query_schemas_builder = query_schemas_builder.bind(status);
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

    let devices = db_devices
        .into_iter()
        .map(|db_device| db_device.transfer())
        .collect::<Result<Vec<_>>>()?;
    Ok((count as usize, devices))
}

pub async fn read_many_on() -> Result<Vec<Device>> {
    let db_devices = sqlx::query_as::<_, DbDevice>(
        format!("SELECT * FROM {} WHERE status = ?", TABLE_NAME).as_str(),
    )
    .bind(Into::<i32>::into(Status::Running))
    .fetch_all(POOL.get().unwrap())
    .await?;
    let devices = db_devices
        .into_iter()
        .map(|db_device| db_device.transfer())
        .collect::<Result<Vec<_>>>()?;

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

pub async fn read_device_type(id: &String) -> Result<i32> {
    let device_type: i32 =
        sqlx::query_scalar(format!("SELECT device_type FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(device_type)
}

pub async fn read_conf_type(id: &String) -> Result<ConfType> {
    let conf_type: i32 =
        sqlx::query_scalar(format!("SELECT conf_type FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    let conf_type: ConfType = conf_type.try_into()?;
    Ok(conf_type)
}

pub async fn count_all() -> Result<usize> {
    let count: i64 = sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

pub async fn read_ids_by_template_id(template_id: &String) -> Result<Vec<String>> {
    let ids: Vec<String> =
        sqlx::query_scalar(format!("SELECT id FROM {} WHERE template_id = ?", TABLE_NAME).as_str())
            .bind(template_id)
            .fetch_all(POOL.get().unwrap())
            .await?;

    Ok(ids)
}

pub async fn update_status(id: &String, status: Status) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET status = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(Into::<i32>::into(status))
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn update_conf(id: &String, req: UpdateReq) -> HaliaResult<()> {
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

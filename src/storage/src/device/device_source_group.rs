use anyhow::Result;
use common::error::{HaliaError, HaliaResult};
use sqlx::prelude::FromRow;
use types::{
    devices::{
        DeviceSourceGroupCreateReq, DeviceSourceGroupQueryParams, DeviceSourceGroupUpdateReq,
    },
    Pagination,
};

use crate::POOL;

const TABLE_NAME: &str = "devices_device_source_groups";

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    device_id CHAR(32) NOT NULL,
    source_group_id CHAR(32) NOT NULL,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

#[derive(FromRow)]
struct DbDeviceSourceGroup {
    pub id: String,
    pub name: String,
    pub device_id: String,
    pub source_group_id: String,
    pub conf: Vec<u8>,
    pub ts: i64,
}

impl DbDeviceSourceGroup {
    pub fn transfer(self) -> Result<DeviceSourceGroup> {
        Ok(DeviceSourceGroup {
            id: self.id,
            name: self.name,
            device_id: self.device_id,
            source_group_id: self.source_group_id,
            conf: serde_json::from_slice(&self.conf)?,
            ts: self.ts,
        })
    }
}

pub struct DeviceSourceGroup {
    pub id: String,
    pub name: String,
    pub device_id: String,
    pub source_group_id: String,
    pub conf: serde_json::Value,
    pub ts: i64,
}

pub async fn insert(
    id: &String,
    device_id: &String,
    req: DeviceSourceGroupCreateReq,
) -> HaliaResult<()> {
    if let Err(err) = sqlx::query(
        format!(
            r#"INSERT INTO {} 
(id, name, device_id, source_group_id, conf, ts) 
VALUES (?, ?, ?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(req.name)
    .bind(device_id)
    .bind(req.source_group_id)
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await
    {
        match err {
            sqlx::Error::Configuration(error) => todo!(),
            sqlx::Error::Database(database_error) => match database_error.code() {
                Some(code) => {
                    if code == "2067" {
                        return Err(common::error::HaliaError::NameExists);
                    } else {
                        todo!()
                    }
                }
                None => todo!(),
            },
            sqlx::Error::Io(error) => return Err(HaliaError::Common(error.to_string())),
            sqlx::Error::Tls(error) => todo!(),
            sqlx::Error::Protocol(_) => todo!(),
            sqlx::Error::RowNotFound => todo!(),
            sqlx::Error::TypeNotFound { type_name } => todo!(),
            sqlx::Error::ColumnIndexOutOfBounds { index, len } => todo!(),
            sqlx::Error::ColumnNotFound(_) => todo!(),
            sqlx::Error::ColumnDecode { index, source } => todo!(),
            sqlx::Error::Encode(error) => todo!(),
            sqlx::Error::Decode(error) => todo!(),
            sqlx::Error::AnyDriverError(error) => todo!(),
            sqlx::Error::PoolTimedOut => todo!(),
            sqlx::Error::PoolClosed => todo!(),
            sqlx::Error::WorkerCrashed => todo!(),
            sqlx::Error::Migrate(migrate_error) => todo!(),
            _ => todo!(),
        }
    }

    Ok(())
}

pub async fn get_by_id(id: &String) -> Result<DeviceSourceGroup> {
    let db_device_source_group = sqlx::query_as::<_, DbDeviceSourceGroup>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_device_source_group.transfer()
}

pub async fn search(
    device_id: &String,
    pagination: Pagination,
    query: DeviceSourceGroupQueryParams,
) -> Result<(usize, Vec<DeviceSourceGroup>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, db_device_source_groups) = match &query.name {
        None => {
            let count: i64 = sqlx::query_scalar(
                format!("SELECT COUNT(*) FROM {} WHERE device_id = ?", TABLE_NAME).as_str(),
            )
            .bind(device_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let db_device_source_groups = sqlx::query_as::<_, DbDeviceSourceGroup>(
                format!(
                    "SELECT * FROM {} WHERE device_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(device_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, db_device_source_groups)
        }
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE device_id = ? AND name LIKE ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(device_id)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let db_device_source_groups = sqlx::query_as::<_, DbDeviceSourceGroup>(
                format!(
                    "SELECT * FROM {} WHERE device_id = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(device_id)
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, db_device_source_groups)
        }
    };

    let device_source_groups = db_device_source_groups
        .into_iter()
        .map(|db_device_source_group| db_device_source_group.transfer())
        .collect::<Result<Vec<_>>>()?;
    Ok((count as usize, device_source_groups))
}

pub async fn read_one(id: &String) -> Result<DeviceSourceGroup> {
    let db_device_source_group = sqlx::query_as::<_, DbDeviceSourceGroup>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_device_source_group.transfer()
}

pub async fn read_one_by_device_id_and_source_group_id(
    device_id: &String,
    source_group_id: &String,
) -> Result<DeviceSourceGroup> {
    let db_device_source_group = sqlx::query_as::<_, DbDeviceSourceGroup>(
        format!(
            "SELECT * FROM {} WHERE device_id = ? AND source_group_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(device_id)
    .bind(source_group_id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_device_source_group.transfer()
}

pub async fn read_conf(id: &String) -> Result<serde_json::Value> {
    let conf: Vec<u8> =
        sqlx::query_scalar(format!("SELECT conf FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    let conf: serde_json::Value = serde_json::from_slice(&conf)?;
    Ok(conf)
}

pub async fn read_device_ids_by_source_group_id(source_group_id: &String) -> Result<Vec<String>> {
    let ids: Vec<String> = sqlx::query_scalar(
        format!(
            "SELECT device_id FROM {} WHERE source_group_id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(source_group_id)
    .fetch_all(POOL.get().unwrap())
    .await?;

    Ok(ids)
}

pub async fn update(id: &String, req: DeviceSourceGroupUpdateReq) -> HaliaResult<()> {
    sqlx::query(format!("UPDATE {} SET name = ?, conf = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(serde_json::to_vec(&req.conf)?)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    crate::delete_by_id(id, TABLE_NAME).await
}

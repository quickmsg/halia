use anyhow::Result;
use common::error::{HaliaError, HaliaResult};
use sqlx::prelude::FromRow;
use types::devices::CreateUpdateDeviceSourceGroupReq;

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
    source_group_id: &String,
    req: CreateUpdateDeviceSourceGroupReq,
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
    .bind(source_group_id)
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

// pub async fn search(
//     pagination: Pagination,
//     query: QueryParams,
// ) -> Result<(usize, Vec<SourceGroup>)> {
//     let (limit, offset) = pagination.to_sql();
//     let (count, db_source_groups) = match (&query.name, &query.device_type) {
//         (None, None) => {
//             let count: i64 =
//                 sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
//                     .fetch_one(POOL.get().unwrap())
//                     .await?;

//             let db_source_groups = sqlx::query_as::<_, DbSourceGroup>(
//                 format!(
//                     "SELECT * FROM {} ORDER BY ts DESC LIMIT ? OFFSET ?",
//                     TABLE_NAME
//                 )
//                 .as_str(),
//             )
//             .bind(limit)
//             .bind(offset)
//             .fetch_all(POOL.get().unwrap())
//             .await?;

//             (count, db_source_groups)
//         }
//         _ => {
//             let mut where_clause = String::new();

//             if query.name.is_some() {
//                 where_clause.push_str("WHERE name LIKE ?")
//             }
//             if query.device_type.is_some() {
//                 match where_clause.is_empty() {
//                     true => where_clause.push_str("WHERE device_type = ?"),
//                     false => where_clause.push_str(" AND device_type = ?"),
//                 }
//             }

//             let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
//             let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
//                 sqlx::query_scalar(&query_count_str);

//             let query_schemas_str = format!(
//                 "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
//                 TABLE_NAME, where_clause
//             );
//             let mut query_schemas_builder: QueryAs<'_, Any, DbSourceGroup, AnyArguments> =
//                 sqlx::query_as::<_, DbSourceGroup>(&query_schemas_str);

//             if let Some(name) = query.name {
//                 let name = format!("%{}%", name);
//                 query_count_builder = query_count_builder.bind(name.clone());
//                 query_schemas_builder = query_schemas_builder.bind(name);
//             }
//             if let Some(device_type) = query.device_type {
//                 let device_type: i32 = device_type.into();
//                 query_count_builder = query_count_builder.bind(device_type);
//                 query_schemas_builder = query_schemas_builder.bind(device_type);
//             }

//             let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
//             let db_source_groups = query_schemas_builder
//                 .bind(limit)
//                 .bind(offset)
//                 .fetch_all(POOL.get().unwrap())
//                 .await?;

//             (count, db_source_groups)
//         }
//     };

//     let source_groups = db_source_groups
//         .into_iter()
//         .map(|db_source_group| db_source_group.transfer())
//         .collect::<Result<Vec<_>>>()?;
//     Ok((count as usize, source_groups))
// }

pub async fn update(id: &String, req: CreateUpdateDeviceSourceGroupReq) -> HaliaResult<()> {
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

use anyhow::Result;
use common::error::{HaliaError, HaliaResult};
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    devices::{
        source_group::{CreateReq, QueryParams, UpdateReq},
        DeviceType,
    },
    Pagination,
};

use crate::POOL;

const TABLE_NAME: &str = "devices_source_groups";

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    device_type SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

#[derive(FromRow)]
struct DbSourceGroup {
    pub id: String,
    pub device_type: i32,
    pub name: String,
    pub ts: i64,
}

impl DbSourceGroup {
    pub fn transfer(self) -> Result<SourceGroup> {
        Ok(SourceGroup {
            id: self.id,
            device_type: self.device_type.try_into()?,
            name: self.name,
            ts: self.ts,
        })
    }
}

pub struct SourceGroup {
    pub id: String,
    pub device_type: DeviceType,
    pub name: String,
    pub ts: i64,
}

pub async fn insert(id: &String, req: CreateReq) -> HaliaResult<()> {
    if let Err(err) = sqlx::query(
        format!(
            r#"INSERT INTO {} 
(id, device_type, name, ts) 
VALUES (?, ?, ?, ?)"#,
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(Into::<i32>::into(req.device_type))
    .bind(req.name)
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

pub async fn get_by_id(id: &String) -> Result<SourceGroup> {
    let db_source_group = sqlx::query_as::<_, DbSourceGroup>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_source_group.transfer()
}

pub async fn search(
    pagination: Pagination,
    query: QueryParams,
) -> Result<(usize, Vec<SourceGroup>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, db_source_groups) = match (&query.name, &query.device_type) {
        (None, None) => {
            let count: i64 =
                sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let db_source_groups = sqlx::query_as::<_, DbSourceGroup>(
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

            (count, db_source_groups)
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

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(&query_count_str);

            let query_schemas_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );
            let mut query_schemas_builder: QueryAs<'_, Any, DbSourceGroup, AnyArguments> =
                sqlx::query_as::<_, DbSourceGroup>(&query_schemas_str);

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
            let db_source_groups = query_schemas_builder
                .bind(limit)
                .bind(offset)
                .fetch_all(POOL.get().unwrap())
                .await?;

            (count, db_source_groups)
        }
    };

    let source_groups = db_source_groups
        .into_iter()
        .map(|db_source_group| db_source_group.transfer())
        .collect::<Result<Vec<_>>>()?;
    Ok((count as usize, source_groups))
}

pub async fn read_device_type(id: &String) -> Result<DeviceType> {
    let device_type: i32 =
        sqlx::query_scalar(format!("SELECT device_type FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    let device_type: DeviceType = device_type.try_into()?;
    Ok(device_type)
}

pub async fn update(id: &String, req: UpdateReq) -> HaliaResult<()> {
    sqlx::query(format!("UPDATE {} SET name = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    crate::delete_by_id(id, TABLE_NAME).await
}

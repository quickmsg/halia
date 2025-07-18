use anyhow::Result;
use common::error::{HaliaError, HaliaResult};
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    schema::{CreateUpdateSchemaReq, ProtocolType, QueryParams, SchemaType},
    Pagination,
};

pub mod reference;

use super::POOL;

const TABLE_NAME: &str = "halia_schemas";

#[derive(FromRow)]
pub struct DbSchema {
    pub id: String,
    pub name: String,
    pub schema_type: i32,
    pub protocol_type: i32,
    pub conf: Vec<u8>,
    pub ts: i64,
}

impl DbSchema {
    pub fn transfer(self) -> Result<Schema> {
        Ok(Schema {
            id: self.id,
            name: self.name,
            schema_type: self.schema_type.try_into()?,
            protocol_type: self.protocol_type.try_into()?,
            conf: serde_json::from_slice(&self.conf)?,
            ts: self.ts,
        })
    }
}

pub struct Schema {
    pub id: String,
    pub name: String,
    pub schema_type: SchemaType,
    pub protocol_type: ProtocolType,
    pub conf: serde_json::Value,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    schema_type SMALLINT NOT NULL,
    protocol_type SMALLINT NOT NULL,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME,
    )
}

pub async fn insert(id: &String, req: CreateUpdateSchemaReq) -> HaliaResult<()> {
    sqlx::query(
        format!(
            "INSERT INTO {} (id, name, schema_type, protocol_type, conf, ts) VALUES (?, ?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(req.name)
    .bind(Into::<i32>::into(req.schema_type))
    .bind(Into::<i32>::into(req.protocol_type))
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<Schema> {
    let db_schema = sqlx::query_as::<_, DbSchema>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_schema.transfer()
}

pub async fn read_conf(id: &String) -> Result<Vec<u8>> {
    let conf: Vec<u8> = sqlx::query_scalar("SELECT conf FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(conf)
}

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Schema>)> {
    let (limit, offset) = pagination.to_sql();

    let mut where_cluase = String::new();
    if query_params.name.is_some() {
        match where_cluase.is_empty() {
            true => where_cluase.push_str("WHERE name LIKE ?"),
            false => where_cluase.push_str(" AND name LIKE ?"),
        }
    }
    if query_params.schema_type.is_some() {
        match where_cluase.is_empty() {
            true => where_cluase.push_str("WHERE schema_type = ?"),
            false => where_cluase.push_str(" AND schema_type = ?"),
        }
    }
    if query_params.protocol_type.is_some() {
        match where_cluase.is_empty() {
            true => where_cluase.push_str("WHERE protocol_type = ?"),
            false => where_cluase.push_str(" AND protocol_type = ?"),
        }
    }

    let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_cluase);
    let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
        sqlx::query_scalar(&query_count_str);

    let query_schemas_str = format!(
        "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
        TABLE_NAME, where_cluase
    );
    let mut query_schemas_builder: QueryAs<'_, Any, DbSchema, AnyArguments> =
        sqlx::query_as::<_, DbSchema>(&query_schemas_str);

    if let Some(name) = query_params.name {
        let name = format!("%{}%", name);
        query_count_builder = query_count_builder.bind(name.clone());
        query_schemas_builder = query_schemas_builder.bind(name);
    }
    if let Some(schema_type) = query_params.schema_type {
        let schema_type: i32 = schema_type.into();
        query_count_builder = query_count_builder.bind(schema_type);
        query_schemas_builder = query_schemas_builder.bind(schema_type);
    }
    if let Some(protocol_type) = query_params.protocol_type {
        let protocol_type: i32 = protocol_type.into();
        query_count_builder = query_count_builder.bind(protocol_type);
        query_schemas_builder = query_schemas_builder.bind(protocol_type);
    }

    let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
    let db_schemas = query_schemas_builder
        .bind(limit)
        .bind(offset)
        .fetch_all(POOL.get().unwrap())
        .await?;

    let schemas = db_schemas
        .into_iter()
        .map(|db_schema| db_schema.transfer())
        .collect::<Result<Vec<_>>>()?;

    Ok((count as usize, schemas))
}

// TODO 更新运行中的源和动作
pub async fn update(id: &String, req: CreateUpdateSchemaReq) -> HaliaResult<()> {
    sqlx::query(
        format!(
            "UPDATE {} SET name = ?, des = ?, conf = ? WHERE id = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(req.name)
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(id)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    let count = reference::count_by_schema_id(id).await?;
    if count > 0 {
        return Err(HaliaError::DeleteRefing);
    }
    super::delete_by_id(id, TABLE_NAME).await
}

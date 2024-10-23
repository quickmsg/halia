use anyhow::Result;
use common::error::{HaliaError, HaliaResult};
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    schema::{CreateUpdateSchemaReq, QueryParams},
    Pagination,
};

pub mod reference;

use super::POOL;

const TABLE_NAME: &str = "halia_schemas";

#[derive(FromRow)]
pub struct Schema {
    pub id: String,
    pub schema_type: i32,
    pub protocol_type: i32,
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
    schema_type SMALLINT NOT NULL,
    protocol_type SMALLINT NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    device_type SMALLINT NOT NULL,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME,
    )
}

pub async fn insert(id: &String, req: CreateUpdateSchemaReq) -> HaliaResult<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = common::timestamp_millis();
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let schema_type: i32 = req.schema_type.into();
    sqlx::query(
        format!(
            "INSERT INTO {} (id, schema_type name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(schema_type)
    .bind(&req.base.name)
    .bind(desc)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<Schema> {
    let schema = sqlx::query_as::<_, Schema>("SELECT * FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(schema)
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
    if query_params.typ.is_some() {
        match where_cluase.is_empty() {
            true => where_cluase.push_str("WHERE typ = ?"),
            false => where_cluase.push_str(" AND typ = ?"),
        }
    }
    if query_params.device_type.is_some() {
        match where_cluase.is_empty() {
            true => where_cluase.push_str("WHERE device_type = ?"),
            false => where_cluase.push_str(" AND device_type = ?"),
        }
    }

    let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_cluase);
    let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
        sqlx::query_scalar(&query_count_str);

    let query_schemas_str = format!(
        "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
        TABLE_NAME, where_cluase
    );
    let mut query_schemas_builder: QueryAs<'_, Any, Schema, AnyArguments> =
        sqlx::query_as::<_, Schema>(&query_schemas_str);

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
    if let Some(device_type) = query_params.device_type {
        let device_type: i32 = device_type.into();
        query_count_builder = query_count_builder.bind(device_type);
        query_schemas_builder = query_schemas_builder.bind(device_type);
    }

    let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
    let schemas = query_schemas_builder
        .bind(limit)
        .bind(offset)
        .fetch_all(POOL.get().unwrap())
        .await?;

    Ok((count as usize, schemas))
}

// TODO 更新运行中的源和动作
pub async fn update(id: &String, req: CreateUpdateSchemaReq) -> HaliaResult<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    sqlx::query("UPDATE devices SET name = ?, des = ?, conf = ? WHERE id = ?")
        .bind(req.base.name)
        .bind(desc)
        .bind(conf)
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

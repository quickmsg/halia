use anyhow::Result;
use common::error::HaliaResult;
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

pub const TABLE_NAME: &str = "halia_schemas";

#[derive(FromRow)]
pub struct Schema {
    pub id: String,
    pub name: String,
    pub typ: i32,
    pub protocol_type: i32,
    // desc为关键字
    pub des: Option<Vec<u8>>,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
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
    sqlx::query(
    format!("INSERT INTO {} (id, status, err, typ, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", TABLE_NAME).as_str()
    )
    .bind(id)
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
    if let Some(protocol_type) = query_params.protocol_type {
        let protocol_type: i32 = protocol_type.into();
        query_count_builder = query_count_builder.bind(protocol_type);
        query_schemas_builder = query_schemas_builder.bind(protocol_type);
    }

    let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
    let schemas = query_schemas_builder
        .bind(limit)
        .bind(offset)
        .fetch_all(POOL.get().unwrap())
        .await?;

    Ok((count as usize, schemas))
}

pub async fn read_name(id: &String) -> Result<String> {
    let name: String = sqlx::query_scalar("SELECT name FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(name)
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

pub async fn delete_by_id(id: &String) -> Result<()> {
    super::delete_by_id(id, TABLE_NAME).await
}

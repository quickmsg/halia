use anyhow::Result;
use common::error::HaliaResult;
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    devices::{CreateUpdateDeviceReq, QueryParams},
    CreateUpdateSourceOrSinkTemplateReq, Pagination,
};

use crate::SourceSinkType;

use super::POOL;

const TABLE_NAME: &str = "device_source_templates";

#[derive(FromRow)]
pub struct SourceTemplate {
    pub id: String,
    pub name: String,
    pub des: Option<Vec<u8>>,
    pub typ: i32,
    pub device_type: i32,
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
    typ SMALLINT UNSIGNED NOT NULL,
    device_type SMALLINT UNSIGNED NOT NULL,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(
    id: &String,
    typ: SourceSinkType,
    req: CreateUpdateSourceOrSinkTemplateReq,
) -> HaliaResult<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = common::timestamp_millis();
    let typ: i32 = typ.into();
    let device_type: i32 = req.device_type.into();
    sqlx::query(
    format!("INSERT INTO {} (id, name, des, typ, device_type, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?)", TABLE_NAME).as_str(),
    )
    .bind(id)
    .bind(req.base.name)
    .bind(req.base.desc)
    .bind(typ)
    .bind(device_type)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

// pub async fn read_one(id: &String) -> Result<Device> {
//     let device = sqlx::query_as::<_, Device>("SELECT * FROM devices WHERE id = ?")
//         .bind(id)
//         .fetch_one(POOL.get().unwrap())
//         .await?;

//     Ok(device)
// }

pub async fn read_conf(id: &String) -> Result<Vec<u8>> {
    let conf: Vec<u8> = sqlx::query_scalar("SELECT conf FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(conf)
}

// pub async fn search(
//     pagination: Pagination,
//     query_params: QueryParams,
// ) -> Result<(usize, Vec<Device>)> {
//     let (limit, offset) = pagination.to_sql();
//     let (count, devices) = match (
//         &query_params.name,
//         &query_params.typ,
//         &query_params.on,
//         &query_params.err,
//     ) {
//         (None, None, None, None) => {
//             let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
//                 .fetch_one(POOL.get().unwrap())
//                 .await?;

//             let devices = sqlx::query_as::<_, Device>(
//                 "SELECT * FROM devices ORDER BY ts DESC LIMIT ? OFFSET ?",
//             )
//             .bind(limit)
//             .bind(offset)
//             .fetch_all(POOL.get().unwrap())
//             .await?;

//             (count, devices)
//         }
//         _ => {
//             let mut where_clause = String::new();

//             if query_params.name.is_some() {
//                 match where_clause.is_empty() {
//                     true => where_clause.push_str("WHERE name LIKE ?"),
//                     false => where_clause.push_str(" AND name LIKE ?"),
//                 }
//             }
//             if query_params.typ.is_some() {
//                 match where_clause.is_empty() {
//                     true => where_clause.push_str("WHERE typ = ?"),
//                     false => where_clause.push_str(" AND typ = ?"),
//                 }
//             }
//             if query_params.on.is_some() {
//                 match where_clause.is_empty() {
//                     true => where_clause.push_str("WHERE status = ?"),
//                     false => where_clause.push_str(" AND status = ?"),
//                 }
//             }
//             if query_params.err.is_some() {
//                 match where_clause.is_empty() {
//                     true => where_clause.push_str("WHERE err = ?"),
//                     false => where_clause.push_str(" AND err = ?"),
//                 }
//             }

//             let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
//             let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
//                 sqlx::query_scalar(&query_count_str);

//             let query_schemas_str = format!(
//                 "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
//                 TABLE_NAME, where_clause
//             );
//             let mut query_schemas_builder: QueryAs<'_, Any, Device, AnyArguments> =
//                 sqlx::query_as::<_, Device>(&query_schemas_str);

//             if let Some(name) = query_params.name {
//                 let name = format!("%{}%", name);
//                 query_count_builder = query_count_builder.bind(name.clone());
//                 query_schemas_builder = query_schemas_builder.bind(name);
//             }
//             if let Some(typ) = query_params.typ {
//                 let typ: i32 = typ.into();
//                 query_count_builder = query_count_builder.bind(typ);
//                 query_schemas_builder = query_schemas_builder.bind(typ);
//             }
//             if let Some(on) = query_params.on {
//                 query_count_builder = query_count_builder.bind(on as i32);
//                 query_schemas_builder = query_schemas_builder.bind(on as i32);
//             }
//             if let Some(err) = query_params.err {
//                 query_count_builder = query_count_builder.bind(err as i32);
//                 query_schemas_builder = query_schemas_builder.bind(err as i32);
//             }

//             let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
//             let devices = query_schemas_builder
//                 .bind(limit)
//                 .bind(offset)
//                 .fetch_all(POOL.get().unwrap())
//                 .await?;

//             (count, devices)
//         }
//     };

//     Ok((count as usize, devices))
// }

// pub async fn read_on() -> Result<Vec<Device>> {
//     let devices = sqlx::query_as::<_, Device>("SELECT * FROM devices WHERE status = 1")
//         .fetch_all(POOL.get().unwrap())
//         .await?;

//     Ok(devices)
// }

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

pub async fn update_status(id: &String, status: bool) -> Result<()> {
    sqlx::query("UPDATE devices SET status = ? WHERE id = ?")
        .bind(status as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn update_err(id: &String, err: bool) -> Result<()> {
    sqlx::query("UPDATE devices SET err = ? WHERE id = ?")
        .bind(err as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
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
    todo!()
    // super::delete_by_id(id, TABLE_NAME).await
}

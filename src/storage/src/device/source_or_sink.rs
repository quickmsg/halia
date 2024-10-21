use anyhow::Result;
use common::error::HaliaResult;
use sqlx::FromRow;
use types::{devices::CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams};

use crate::SourceSinkType;

use super::POOL;

static TABLE_NAME: &str = "device_source_or_sink";

#[derive(FromRow)]
pub struct SourceOrSink {
    pub id: String,
    pub name: String,
    pub des: Option<Vec<u8>>,
    pub typ: i32,
    pub parent_id: String,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    device_id CHAR(32) NOT NULL,
    name VARCHAR(255) NOT NULL,
    des BLOB,
    typ SMALLINT UNSIGNED NOT NULL,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL,
    UNIQUE (device_id, typ, name)
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(
    id: &String,
    device_id: &String,
    typ: SourceSinkType,
    req: CreateUpdateSourceOrSinkReq,
) -> Result<()> {
    let typ: i32 = typ.into();
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = common::timestamp_millis();
    let desc = match req.base.desc {
        Some(desc) => Some(desc.as_bytes().to_vec()),
        None => None,
    };

    sqlx::query(
        format!(
            "INSERT INTO {} (id, name, des, typ, device_id, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(req.base.name)
    .bind(desc)
    .bind(typ)
    .bind(device_id)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

// pub async fn read_all_by_parent_id(parent_id: &String, typ: Type) -> Result<Vec<SourceOrSink>> {
//     let typ: i32 = typ.into();
//     let sources_or_sinks = sqlx::query_as::<_, SourceOrSink>(
//         "SELECT * FROM sources_or_sinks WHERE parent_id = ? AND typ = ? ORDER BY ts DESC",
//     )
//     .bind(parent_id)
//     .bind(typ)
//     .fetch_all(POOL.get().unwrap())
//     .await?;
//     Ok(sources_or_sinks)
// }

pub async fn read_one(id: &String) -> Result<SourceOrSink> {
    let source_or_sink =
        sqlx::query_as::<_, SourceOrSink>("SELECT * FROM sources_or_sinks WHERE id = ?")
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;
    Ok(source_or_sink)
}

pub async fn search(
    device_id: &String,
    typ: SourceSinkType,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> Result<(usize, Vec<SourceOrSink>)> {
    let typ: i32 = typ.into();
    let (limit, offset) = pagination.to_sql();
    let (count, sources_or_sinks) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE device_id = ? AND typ = ? AND name LIKE ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(device_id)
            .bind(typ)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, SourceOrSink>(
                format!("SELECT * FROM {} WHERE device_id = ? AND typ = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            ).bind(device_id)
            .bind(typ)
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap()).await?;

            (count, sources_or_sinks)
        }
        None => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE device_id = ? AND typ = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(device_id)
            .bind(typ)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, SourceOrSink>(
                format!("SELECT * FROM {} WHERE device_id = ? AND typ = ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            )
            .bind(device_id)
            .bind(typ)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, sources_or_sinks)
        }
    };

    Ok((count as usize, sources_or_sinks))
}

pub async fn count_by_deviec_id(device_id: &String, typ: SourceSinkType) -> Result<usize> {
    let typ: i32 = typ.into();
    let count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM {} WHERE device_id = ? AND typ = ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(device_id)
    .bind(typ)
    .fetch_one(POOL.get().unwrap())
    .await?;
    Ok(count as usize)
}

pub async fn read_conf(id: &String) -> Result<serde_json::Value> {
    let conf: Vec<u8> = sqlx::query_scalar("SELECT conf FROM sources_or_sinks WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;
    Ok(serde_json::from_slice(&conf)?)
}

pub async fn update(id: &String, req: CreateUpdateSourceOrSinkReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    let desc = match req.base.desc {
        Some(desc) => Some(desc.as_bytes().to_vec()),
        None => None,
    };
    sqlx::query("UPDATE sources_or_sinks SET name = ?, des = ?, conf = ? WHERE id = ?")
        .bind(req.base.name)
        .bind(desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete_by_parent_id(parent_id: &String) -> Result<()> {
    sqlx::query("DELETE FROM sources_or_sinks WHERE parent_id = ?")
        .bind(parent_id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn check_exists(id: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sources_or_sinks WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count == 1)
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    crate::delete_by_id(id, TABLE_NAME).await
}

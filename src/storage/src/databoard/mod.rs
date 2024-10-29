use anyhow::Result;
use sqlx::prelude::FromRow;
use types::{
    databoard::{CreateUpdateDataboardReq, QueryParams},
    Pagination,
};

pub mod data;

use super::POOL;

pub static TABLE_NAME: &str = "databoards";

#[derive(FromRow)]
pub struct Databoard {
    pub id: String,
    pub status: i32,
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
    status SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateUpdateDataboardReq) -> Result<()> {
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = common::timestamp_millis() as i64;
    sqlx::query(
        "INSERT INTO databoards (id, status, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(false as i32)
    .bind(req.base.name)
    .bind(desc)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;
    Ok(())
}

pub async fn query(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Databoard>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, databoards) = match (query_params.name, query_params.on) {
        (None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM databoards")
                .fetch_one(POOL.get().unwrap())
                .await?;

            let databoards = sqlx::query_as::<_, Databoard>(
                "SELECT * FROM databoards ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, databoards)
        }
        (None, Some(on)) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM databoards WHERE status = ?")
                .bind(on as i32)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let databoards = sqlx::query_as::<_, Databoard>(
                "SELECT * FROM databoards WHERE status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, databoards)
        }
        (Some(name), None) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM databoards WHERE name LIKE ?")
                    .bind(format!("%{}%", name))
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let databoards = sqlx::query_as::<_, Databoard>(
                "SELECT * FROM databoards WHERE name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, databoards)
        }
        (Some(name), Some(on)) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM databoards WHERE name LIKE ? AND status = ?",
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let databoards = sqlx::query_as::<_, Databoard>(
                "SELECT * FROM databoards WHERE name LIKE ? AND status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, databoards)
        }
    };

    Ok((count, databoards))
}

pub async fn read_one(id: &String) -> Result<Databoard> {
    let databoard = sqlx::query_as::<_, Databoard>("SELECT * FROM databoards WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(databoard)
}

pub async fn read_name(id: &String) -> Result<String> {
    let name: String = sqlx::query_scalar("SELECT name FROM databoards WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(name)
}

pub async fn read_many_on() -> Result<Vec<Databoard>> {
    let databoards = sqlx::query_as::<_, Databoard>("SELECT * FROM databoards WHERE status = 1")
        .fetch_all(POOL.get().unwrap())
        .await?;

    Ok(databoards)
}

pub async fn count() -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM databoards")
        .fetch_one(POOL.get().unwrap())
        .await?;
    Ok(count as usize)
}

pub async fn update_conf(id: &String, req: CreateUpdateDataboardReq) -> Result<()> {
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf = serde_json::to_vec(&req.ext)?;
    sqlx::query("UPDATE databoards SET name = ?, desc = ?, conf = ? WHERE id = ?")
        .bind(req.base.name)
        .bind(desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn update_status(id: &String, status: bool) -> Result<()> {
    sqlx::query("UPDATE databoards SET status = ? WHERE id = ?")
        .bind(status as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete_by_id(id: &String) -> Result<()> {
    sqlx::query("DELETE FROM databoards WHERE id = ?")
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    data::delete_many(id).await?;

    Ok(())
}

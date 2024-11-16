use anyhow::Result;
use sqlx::prelude::FromRow;
use types::{
    databoard::{CreateUpdateDataboardReq, QueryParams},
    Pagination, Status,
};

pub mod data;

use super::POOL;

static TABLE_NAME: &str = "databoards";

#[derive(FromRow)]
struct DbDataboard {
    pub id: String,
    pub status: i32,
    pub name: String,
    pub conf: Vec<u8>,
    pub ts: i64,
}

impl DbDataboard {
    pub fn transfer(self) -> Result<Databoard> {
        Ok(Databoard {
            id: self.id,
            status: self.status.try_into()?,
            name: self.name,
            conf: serde_json::from_slice(&self.conf)?,
            ts: self.ts,
        })
    }
}

pub struct Databoard {
    pub id: String,
    pub status: Status,
    pub name: String,
    pub conf: serde_json::Value,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    status SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateUpdateDataboardReq) -> Result<()> {
    sqlx::query(
        format!(
            "INSERT INTO {} (id, status, name, conf, ts) VALUES (?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(Into::<i32>::into(Status::default()))
    .bind(req.name)
    .bind(serde_json::to_vec(&req.ext)?)
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;
    Ok(())
}

pub async fn query(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Databoard>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, db_databoards) = match (query_params.name, query_params.on) {
        (None, None) => {
            let count: i64 =
                sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let databoards = sqlx::query_as::<_, DbDataboard>(
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

            (count as usize, databoards)
        }
        (None, Some(on)) => {
            let count: i64 = sqlx::query_scalar(
                format!("SELECT COUNT(*) FROM {} WHERE status = ?", TABLE_NAME).as_str(),
            )
            .bind(on as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let databoards = sqlx::query_as::<_, DbDataboard>(
                format!(
                    "SELECT * FROM {} WHERE status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, databoards)
        }
        (Some(name), None) => {
            let count: i64 = sqlx::query_scalar(
                format!("SELECT COUNT(*) FROM {} WHERE name LIKE ?", TABLE_NAME).as_str(),
            )
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let databoards = sqlx::query_as::<_, DbDataboard>(
                format!(
                    "SELECT * FROM {} WHERE name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
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
                format!(
                    "SELECT COUNT(*) FROM {} WHERE name LIKE ? AND status = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let databoards = sqlx::query_as::<_, DbDataboard>(
            format!("SELECT * FROM {} WHERE name LIKE ? AND status = ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
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

    let databoards = db_databoards
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<_>>>()?;

    Ok((count, databoards))
}

pub async fn read_one(id: &String) -> Result<Databoard> {
    let db_databoard = sqlx::query_as::<_, DbDataboard>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_databoard.transfer()
}

pub async fn read_name(id: &String) -> Result<String> {
    let name: String =
        sqlx::query_scalar(format!("SELECT name FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(name)
}

pub async fn read_all_running() -> Result<Vec<Databoard>> {
    let db_databoards = sqlx::query_as::<_, DbDataboard>(
        format!("SELECT * FROM {} WHERE status = ?", TABLE_NAME).as_str(),
    )
    .bind(Into::<i32>::into(Status::Running))
    .fetch_all(POOL.get().unwrap())
    .await?;

    db_databoards.into_iter().map(|x| x.transfer()).collect()
}

pub async fn count() -> Result<usize> {
    let count: i64 = sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
        .fetch_one(POOL.get().unwrap())
        .await?;
    Ok(count as usize)
}

pub async fn update_conf(id: &String, req: CreateUpdateDataboardReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    sqlx::query(format!("UPDATE {} SET name = ?, conf = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn update_status(id: &String, status: Status) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET status = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(Into::<i32>::into(status))
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete_by_id(id: &String) -> Result<()> {
    sqlx::query(format!("DELETE FROM {} WHERE id = ?", TABLE_NAME).as_str())
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    data::delete_many(id).await?;

    Ok(())
}

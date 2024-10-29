use anyhow::Result;
use common::error::HaliaResult;
use sqlx::prelude::FromRow;
use types::{
    rules::{CreateUpdateRuleReq, QueryParams},
    Pagination,
};

use super::POOL;

pub mod reference;

static TABLE_NAME: &str = "rules";

#[derive(FromRow)]
pub struct Rule {
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
    status SMALLINT NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateUpdateRuleReq) -> Result<()> {
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = common::timestamp_millis() as i64;
    sqlx::query("INSERT INTO rules (id, status, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?)")
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

pub async fn read_one(id: &String) -> Result<Rule> {
    let rule = sqlx::query_as::<_, Rule>("SELECT * FROM rules WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(rule)
}

pub async fn read_conf(id: &String) -> Result<Vec<u8>> {
    let conf: Vec<u8> = sqlx::query_scalar("SELECT conf FROM rules WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(conf)
}

pub async fn read_all_on() -> Result<Vec<Rule>> {
    let rules = sqlx::query_as::<_, Rule>("SELECT * FROM rules WHERE status = 1")
        .fetch_all(POOL.get().unwrap())
        .await?;

    Ok(rules)
}

pub async fn query(pagination: Pagination, query: QueryParams) -> Result<(usize, Vec<Rule>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, rules) = match (query.name, query.on) {
        (None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rules")
                .fetch_one(POOL.get().unwrap())
                .await?;

            let rules =
                sqlx::query_as::<_, Rule>("SELECT * FROM rules ORDER BY ts DESC LIMIT ? OFFSET ?")
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(POOL.get().unwrap())
                    .await?;

            (count as usize, rules)
        }
        (None, Some(on)) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rules WHERE status = ?")
                .bind(on as i32)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let rules = sqlx::query_as::<_, Rule>(
                "SELECT * FROM rules WHERE status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, rules)
        }
        (Some(name), None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rules WHERE name LIKE ?")
                .bind(format!("%{}%", name))
                .fetch_one(POOL.get().unwrap())
                .await?;

            let rules = sqlx::query_as::<_, Rule>(
                "SELECT * FROM rules WHERE name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, rules)
        }
        (Some(name), Some(on)) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM rules WHERE name LIKE ? AND status = ?")
                    .bind(format!("%{}%", name))
                    .bind(on as i32)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let rules = sqlx::query_as::<_, Rule>(
                "SELECT * FROM rules WHERE name LIKE ? AND status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, rules)
        }
    };

    Ok((count, rules))
}

pub async fn update_status(id: &String, status: bool) -> Result<()> {
    sqlx::query("UPDATE rules SET status = ? WHERE id = ?")
        .bind(status as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn update(id: &String, req: CreateUpdateRuleReq) -> Result<()> {
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf = serde_json::to_vec(&req.ext)?;
    sqlx::query("UPDATE rules SET name = ?, des = ?, conf = ? WHERE id = ?")
        .bind(req.base.name)
        .bind(desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    super::delete_by_id(id, TABLE_NAME).await
}

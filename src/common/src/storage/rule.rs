use anyhow::Result;
use sqlx::prelude::FromRow;
use types::rules::CreateUpdateRuleReq;

use super::POOL;

#[derive(FromRow)]
pub struct Rule {
    pub id: String,
    pub status: i32,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
    pub ts: i64,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS rules (
    id VARCHAR(255) PRIMARY KEY,      -- 使用 VARCHAR(255) 来适配 MySQL 和 SQLite
    status INTEGER NOT NULL,          -- 状态字段使用 INTEGER
    name TEXT NOT NULL,               -- 名称字段使用 TEXT
    `desc` TEXT,                      -- `desc` 是保留字，使用反引号括起来
    conf TEXT NOT NULL                -- 配置字段使用 TEXT
);
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn insert(id: &String, conf: String) -> Result<()> {
    let ts = chrono::Utc::now().timestamp();
    sqlx::query("INSERT INTO rules (id, status, conf, ts) VALUES (?1, ?2, ?3)")
        .bind(id)
        .bind(false as i32)
        .bind(conf)
        .bind(ts)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn read_all() -> Result<Vec<Rule>> {
    let rules = sqlx::query_as::<_, Rule>("SELECT * FROM rules")
        .fetch_all(POOL.get().unwrap())
        .await?;

    Ok(rules)
}

pub async fn update_status(id: &String, status: bool) -> Result<()> {
    sqlx::query("UPDATE rules SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn update(id: &String, req: CreateUpdateRuleReq) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    sqlx::query("UPDATE rules SET name = ?1, desc = ?2, conf = ?3 WHERE id = ?4")
        .bind(req.base.name)
        .bind(req.base.desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete(id: &String) -> Result<()> {
    sqlx::query("DELETE FROM rules WHERE id = ?1")
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

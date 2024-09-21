use anyhow::Result;
use sqlx::prelude::FromRow;

use super::POOL;

// active 1为引用，2为激活
#[derive(FromRow)]
pub struct RuleRef {
    pub rule_id: String,
    pub parent_id: String,
    pub resource_id: String,
    pub active: i32,
}

pub(crate) async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS rule_refs (
    rule_id VARCHAR(255) NOT NULL,     -- 使用 VARCHAR(255) 来适配 MySQL 和 SQLite
    parent_id VARCHAR(255) NOT NULL,   -- 父 ID 使用 VARCHAR(255)
    resource_id VARCHAR(255) NOT NULL, -- 资源 ID 使用 VARCHAR(255)
    active INT NOT NULL                -- 使用 INT 来表示布尔状态
);
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn insert(rule_id: &String, parent_id: &String, resource_id: &String) -> Result<()> {
    sqlx::query(
        "INSERT INTO rule_refs (rule_id, parent_id, resource_id, active) VALUES (?1, ?2, ?3, ?4)",
    )
    .bind(rule_id)
    .bind(parent_id)
    .bind(resource_id)
    .bind(1)
    .execute(POOL.get().unwrap())
    .await?;
    Ok(())
}

pub async fn active(rule_id: &String) -> Result<()> {
    sqlx::query("UPDATE rule_refs SET active = 2 WHERE rule_id = ?1")
        .bind(rule_id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn deactive(rule_id: &String) -> Result<()> {
    sqlx::query("UPDATE rule_refs SET active = 1 WHERE rule_id = ?1")
        .bind(rule_id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete_many_by_rule_id(rule_id: &String) -> Result<()> {
    sqlx::query("DELETE FROM rule_refs WHERE rule_id = ?1")
        .bind(rule_id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn count_cnt_by_parent_id(parent_id: &String) -> Result<usize> {
    let cnt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE parent_id = ?1")
        .bind(parent_id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(cnt as usize)
}

pub async fn count_active_cnt_by_parent_id(parent_id: &String) -> Result<usize> {
    let active_cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE active = 2 AND parent_id = ?1")
            .bind(parent_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(active_cnt as usize)
}

pub async fn count_cnt_by_resource_id(resource_id: &String) -> Result<usize> {
    let cnt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE resource_id = ?1")
        .bind(resource_id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(cnt as usize)
}

pub async fn count_active_cnt_by_resource_id(resource_id: &String) -> Result<usize> {
    let active_cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE active = 2 AND resource_id = ?1")
            .bind(resource_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(active_cnt as usize)
}

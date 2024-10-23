use anyhow::Result;
use sqlx::{prelude::FromRow, query_builder};

use super::POOL;

static TABLE_NAME: &str = "rule_refs";

#[derive(FromRow)]
pub struct RuleRef {
    pub rule_id: String,
    pub parent_id: String,
    pub resource_id: String,
    pub active: i32,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    rule_id CHAR(32) NOT NULL,
    parent_id CHAR(32) NOT NULL,
    resource_id CHAR(32) NOT NULL,
    active SMALLINT NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(rule_id: &String, parent_id: &String, resource_id: &String) -> Result<()> {
    sqlx::query(
        "INSERT INTO rule_refs (rule_id, parent_id, resource_id, active) VALUES (?, ?, ?, ?)",
    )
    .bind(rule_id)
    .bind(parent_id)
    .bind(resource_id)
    .bind(false as i32)
    .execute(POOL.get().unwrap())
    .await?;
    Ok(())
}

pub async fn active(rule_id: &String) -> Result<()> {
    sqlx::query("UPDATE rule_refs SET active = ? WHERE rule_id = ?")
        .bind(true as i32)
        .bind(rule_id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn deactive(rule_id: &String) -> Result<()> {
    sqlx::query("UPDATE rule_refs SET active = ? WHERE rule_id = ?")
        .bind(false as i32)
        .bind(rule_id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete_many_by_rule_id(rule_id: &String) -> Result<()> {
    sqlx::query("DELETE FROM rule_refs WHERE rule_id = ?")
        .bind(rule_id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn count_cnt_by_parent_id(parent_id: &String) -> Result<usize> {
    let cnt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE parent_id = ?")
        .bind(parent_id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(cnt as usize)
}

pub async fn count_active_cnt_by_parent_id(parent_id: &String) -> Result<usize> {
    let active_cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE active = ? AND parent_id = ?")
            .bind(true as i32)
            .bind(parent_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(active_cnt as usize)
}

pub async fn count_cnt_by_resource_id(resource_id: &String) -> Result<usize> {
    let cnt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE resource_id = ?")
        .bind(resource_id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(cnt as usize)
}

pub async fn count_cnt_by_many_resource_ids(resource_ids: &Vec<String>) -> Result<usize> {
    let mut clause = format!("SELECT COUNT(*) FROM {} WHERE resource_id IN (", TABLE_NAME,);
    for resource_id in resource_ids {
        clause.push_str(format!("{},", resource_id).as_str());
    }
    clause.push_str(")");

    let count: i64 = sqlx::query_scalar(clause.as_str())
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

pub async fn count_active_cnt_by_resource_id(resource_id: &String) -> Result<usize> {
    let active_cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE active = ? AND resource_id = ?")
            .bind(true as i32)
            .bind(resource_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(active_cnt as usize)
}

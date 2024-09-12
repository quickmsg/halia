use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use uuid::Uuid;

// active 1为引用，2为激活

#[derive(FromRow)]
pub struct RuleRef {
    pub id: String,
    pub conf: String,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS rule_refs (
    rule_id TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    active INT NOT NULL,
);
"#,
    )
    .execute(storage)
    .await?;

    Ok(())
}

// todo
pub async fn create(storage: &AnyPool, rule_id: &Uuid, resource_id: &Uuid) -> Result<()> {
    sqlx::query("INSERT INTO rule_refs (rule_id, resource_id, active) VALUES (?1, ?2, ?3)")
        .bind(rule_id.to_string())
        .bind(resource_id.to_string())
        .bind(1)
        .execute(storage)
        .await?;
    Ok(())
}

pub async fn active(storage: &AnyPool, rule_id: &Uuid) -> Result<()> {
    sqlx::query("UPDATE rule_refs SET active = 2 WHERE rule_id = ?1")
        .bind(rule_id.to_string())
        .execute(storage)
        .await?;

    Ok(())
}

pub async fn deactive(storage: &AnyPool, rule_id: &Uuid) -> Result<()> {
    sqlx::query("UPDATE rule_refs SET active = 1 WHERE rule_id = ?1")
        .bind(rule_id.to_string())
        .execute(storage)
        .await?;

    Ok(())
}

pub async fn delete(storage: &AnyPool, rule_id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM rule_refs WHERE rule_id = ?1")
        .bind(rule_id.to_string())
        .execute(storage)
        .await?;

    Ok(())
}

pub async fn count_cnt_by_resource_id(storage: &AnyPool, resource_id: &Uuid) -> Result<usize> {
    let cnt: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE resource_id = ?1")
        .bind(resource_id.to_string())
        .fetch_one(storage)
        .await?;

    Ok(cnt as usize)
}

pub async fn count_active_cnt_by_resource_id(
    storage: &AnyPool,
    resource_id: &Uuid,
) -> Result<usize> {
    let active_cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE active = 2 AND resource_id = ?1")
            .bind(resource_id.to_string())
            .fetch_one(storage)
            .await?;

    Ok(active_cnt as usize)
}
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
    source_or_sink_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    active INT NOT NULL,
);
"#,
    )
    .execute(storage)
    .await?;

    Ok(())
}

// todo
pub async fn create(storage: &AnyPool, source_or_sink_id: &Uuid, rule_id: &Uuid) -> Result<()> {
    sqlx::query("INSERT INTO rule_refs (source_or_sink_id, rule_id, active) VALUES (?1, ?2, ?3)")
        .bind(source_or_sink_id.to_string())
        .bind(rule_id.to_string())
        .bind(1)
        .execute(storage)
        .await?;
    Ok(())
}

pub async fn active(storage: &AnyPool, source_or_sink_id: &Uuid, rule_id: &Uuid) -> Result<()> {
    sqlx::query("UPDATE rule_refs SET active = 2 WHERE source_or_sink_id = ?1 AND rule_id = ?2")
        .bind(source_or_sink_id.to_string())
        .bind(rule_id.to_string())
        .execute(storage)
        .await?;

    Ok(())
}

pub async fn deactive(storage: &AnyPool, source_or_sink_id: &Uuid, rule_id: &Uuid) -> Result<()> {
    sqlx::query("UPDATE rule_refs SET active = 1 WHERE source_or_sink_id = ?1 AND rule_id = ?2")
        .bind(source_or_sink_id.to_string())
        .bind(rule_id.to_string())
        .execute(storage)
        .await?;

    Ok(())
}

pub async fn delete(storage: &AnyPool, source_or_sink_id: &Uuid, rule_id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM rule_refs WHERE source_or_sink_id = ?1 AND rule_id = ?2")
        .bind(source_or_sink_id.to_string())
        .bind(rule_id.to_string())
        .execute(storage)
        .await?;

    Ok(())
}

pub async fn count_cnt(storage: &AnyPool, source_or_sink_id: &Uuid) -> Result<usize> {
    let cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE source_or_sink_id = ?1")
            .bind(source_or_sink_id.to_string())
            .fetch_one(storage)
            .await?;

    Ok(cnt as usize)
}

pub async fn count_active_cnt(storage: &AnyPool, rule_id: &Uuid) -> Result<usize> {
    let active_cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE active = 2 AND rule_id = ?1")
            .bind(rule_id.to_string())
            .fetch_one(storage)
            .await?;

    Ok(active_cnt as usize)
}

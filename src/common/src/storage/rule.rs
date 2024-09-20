use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use types::rules::CreateUpdateRuleReq;
use uuid::Uuid;

#[derive(FromRow)]
pub struct Rule {
    pub id: String,
    pub status: i32,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS rules (
    id TEXT PRIMARY KEY,
    status INTEGER NOT NULL,
    conf TEXT NOT NULL
);
"#,
    )
    .execute(storage)
    .await?;

    Ok(())
}

pub async fn create_rule(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("INSERT INTO rules (id, status, conf) VALUES (?1, ?2, ?3)")
        .bind(id.to_string())
        .bind(false as i32)
        .bind(conf)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn read_rules(pool: &AnyPool) -> Result<Vec<Rule>> {
    let rules = sqlx::query_as::<_, Rule>("SELECT id, status, conf FROM rules")
        .fetch_all(pool)
        .await?;

    Ok(rules)
}

pub async fn update_rule_status(pool: &AnyPool, id: &Uuid, status: bool) -> Result<()> {
    sqlx::query("UPDATE rules SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_rule_conf(pool: &AnyPool, id: &Uuid, req: CreateUpdateRuleReq) -> Result<()> {
    sqlx::query("UPDATE rules SET conf = ?1 WHERE id = ?2")
        // .bind(conf)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_rule(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM rules WHERE id = ?1")
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

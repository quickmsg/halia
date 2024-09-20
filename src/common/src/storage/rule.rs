use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use types::rules::CreateUpdateRuleReq;

#[derive(FromRow)]
pub struct Rule {
    pub id: String,
    pub status: i32,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
    pub ts: i64,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS rules (
    id TEXT PRIMARY KEY,
    status INTEGER NOT NULL,
    name TEXT NOT NULL,
    desc TEXT,
    conf TEXT NOT NULL
);
"#,
    )
    .execute(storage)
    .await?;

    Ok(())
}

pub async fn insert(storage: &AnyPool, id: &String, conf: String) -> Result<()> {
    let ts = chrono::Utc::now().timestamp();
    sqlx::query("INSERT INTO rules (id, status, conf, ts) VALUES (?1, ?2, ?3)")
        .bind(id)
        .bind(false as i32)
        .bind(conf)
        .bind(ts)
        .execute(storage)
        .await?;
    Ok(())
}

pub async fn read_all(storage: &AnyPool) -> Result<Vec<Rule>> {
    let rules = sqlx::query_as::<_, Rule>("SELECT * FROM rules")
        .fetch_all(storage)
        .await?;

    Ok(rules)
}

pub async fn update_status(storage: &AnyPool, id: &String, status: bool) -> Result<()> {
    sqlx::query("UPDATE rules SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id)
        .execute(storage)
        .await?;
    Ok(())
}

pub async fn update(storage: &AnyPool, id: &String, req: CreateUpdateRuleReq) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    sqlx::query("UPDATE rules SET name = ?1, desc = ?2, conf = ?3 WHERE id = ?4")
        .bind(req.base.name)
        .bind(req.base.desc)
        .bind(conf)
        .bind(id)
        .execute(storage)
        .await?;
    Ok(())
}

pub async fn delete(storage: &AnyPool, id: &String) -> Result<()> {
    sqlx::query("DELETE FROM rules WHERE id = ?1")
        .bind(id)
        .execute(storage)
        .await?;
    Ok(())
}

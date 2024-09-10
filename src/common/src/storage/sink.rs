use anyhow::Result;
use sqlx::{AnyPool, FromRow};
use types::CreateUpdateSourceOrSinkReq;
use uuid::Uuid;

#[derive(FromRow, Debug)]
pub struct Sink {
    pub id: String,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS sinks (
    id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
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

pub async fn create_sink(
    pool: &AnyPool,
    parent_id: &Uuid,
    id: &Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    match req.base.desc {
        Some(desc) => {
            sqlx::query(
                r#"INSERT INTO sinks (id, parent_id, name, desc, conf) VALUES (?1, ?2, ?3 ?4 ?5)"#,
            )
            .bind(id.to_string())
            .bind(parent_id.to_string())
            .bind(req.base.name)
            .bind(desc)
            .bind(conf)
            .execute(pool)
            .await?;
        }
        None => {
            sqlx::query(r#"INSERT INTO sinks (id, parent_id, name, conf) VALUES (?1, ?2, ?3 ?4)"#)
                .bind(id.to_string())
                .bind(parent_id.to_string())
                .bind(req.base.name)
                .bind(conf)
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

pub async fn read_sinks(storage: &AnyPool, parent_id: &Uuid) -> Result<Vec<Sink>> {
    Ok(
        sqlx::query_as::<_, Sink>(r#"SELECT * FROM sinks WHERE parent_id = ?1"#)
            .bind(parent_id.to_string())
            .fetch_all(storage)
            .await?,
    )
}

pub async fn count_sinks_by_parent_id(pool: &AnyPool, parent_id: &Uuid) -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sinks WHERE parent_id = ?1")
        .bind(parent_id.to_string())
        .fetch_one(pool)
        .await?;
    Ok(count as usize)
}

pub async fn update_sink(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("UPDATE sinks SET conf = ?1 WHERE id = ?2")
        .bind(conf)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_sink(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query("DELETE sinks WHERE id = ?1")
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

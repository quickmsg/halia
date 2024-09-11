use anyhow::Result;
use sqlx::{AnyPool, FromRow};
use types::{CreateUpdateSourceOrSinkReq, Pagination};
use uuid::Uuid;

#[derive(FromRow)]
pub struct Source {
    pub id: String,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
    name TEXT NOT NULL,
    desc TEXT,
    conf TEXT NOT NULL,
    ts INT NOT NULL
);
"#,
    )
    .execute(storage)
    .await?;

    Ok(())
}

pub async fn create_source(
    pool: &AnyPool,
    parent_id: &Uuid,
    id: &Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    let ts = chrono::Utc::now().timestamp();
    match req.base.desc {
        Some(desc) => {
            sqlx::query(
                r#"INSERT INTO sources (id, parent_id, name, desc, conf, ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
            )
            .bind(id.to_string())
            .bind(parent_id.to_string())
            .bind(req.base.name)
            .bind(desc)
            .bind(conf)
            .bind(ts)
            .execute(pool)
            .await?;
        }
        None => {
            sqlx::query(
                r#"INSERT INTO sources (id, parent_id, name, conf, ts) VALUES (?1, ?2, ?3, ?4, ?5)"#,
            )
            .bind(id.to_string())
            .bind(parent_id.to_string())
            .bind(req.base.name)
            .bind(conf)
            .bind(ts)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

pub async fn read_sources(pool: &AnyPool, parent_id: &Uuid) -> Result<Vec<Source>> {
    Ok(
        sqlx::query_as::<_, Source>("SELECT * FROM sources WHERE parent_id = ?1")
            .bind(parent_id.to_string())
            .fetch_all(pool)
            .await?,
    )
}

pub async fn search_sources(
    storage: &AnyPool,
    parent_id: &Uuid,
    pagination: Pagination,
) -> Result<(usize, Vec<Source>)> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sources")
        .fetch_one(storage)
        .await?;
    let sources = sqlx::query_as::<_, Source>(
        r#"
SELECT * FROM sources
WHERE parent_id = ?1 
ORDER BY ts DESC 
LIMIT ?2 OFFSET ?3"#,
    )
    .bind(parent_id.to_string())
    .bind(pagination.size as i64)
    .bind(((pagination.page - 1) * pagination.size) as i64)
    .fetch_all(storage)
    .await?;

    Ok((count as usize, sources))
}

pub async fn count_sources_by_parent_id(pool: &AnyPool, parent_id: &Uuid) -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sources WHERE parent_id = ?1")
        .bind(parent_id.to_string())
        .fetch_one(pool)
        .await?;
    Ok(count as usize)
}

pub async fn read_source_conf(pool: &AnyPool, id: &Uuid) -> Result<String> {
    let conf: String = sqlx::query_scalar("SELECT conf FROM sources WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(pool)
        .await?;
    Ok(conf)
}

pub async fn update_source(
    pool: &AnyPool,
    id: &Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    match req.base.desc {
        Some(desc) => {
            sqlx::query("UPDATE sources SET name = ?1, desc = ?2, conf = ?3 WHERE id = ?4")
                .bind(req.base.name)
                .bind(desc)
                .bind(conf)
                .bind(id.to_string())
                .execute(pool)
                .await?;
        }
        None => {
            sqlx::query("UPDATE sources SET name = ?1, conf = ?3 WHERE id = ?4")
                .bind(req.base.name)
                .bind(conf)
                .bind(id.to_string())
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

pub async fn delete_source(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM sources WHERE id = ?1")
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

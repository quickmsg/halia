use anyhow::Result;
use sqlx::{AnyPool, FromRow};
use types::{CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams};
use uuid::Uuid;

#[derive(FromRow, Debug)]
pub struct Sink {
    pub id: String,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
    pub rule_ref_cnt: i32,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS sinks (
    id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
    name TEXT NOT NULL,
    desc TEXT,
    conf TEXT NOT NULL,
    rule_ref_cnt INT NOT NULL
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
                r#"INSERT INTO sinks (id, parent_id, name, desc, conf, ref) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
            )
            .bind(id.to_string())
            .bind(parent_id.to_string())
            .bind(req.base.name)
            .bind(desc)
            .bind(conf)
            .bind(0)
            .execute(pool)
            .await?;
        }
        None => {
            sqlx::query(
                r#"INSERT INTO sinks (id, parent_id, name, conf) VALUES (?1, ?2, ?3, ?4, ?5)"#,
            )
            .bind(id.to_string())
            .bind(parent_id.to_string())
            .bind(req.base.name)
            .bind(conf)
            .bind(0)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}

pub async fn read_all_sinks(storage: &AnyPool, parent_id: &Uuid) -> Result<Vec<Sink>> {
    let sinks = sqlx::query_as::<_, Sink>(r#"SELECT * FROM sinks WHERE parent_id = ?1"#)
        .bind(parent_id.to_string())
        .fetch_all(storage)
        .await?;

    Ok(sinks)
}

pub async fn read_sink(storage: &AnyPool, id: &Uuid) -> Result<Sink> {
    let sink = sqlx::query_as::<_, Sink>("SELECT * FROM sinks WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(storage)
        .await?;

    Ok(sink)
}

pub async fn search_sinks(
    storage: &AnyPool,
    parent_id: &Uuid,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> Result<(usize, Vec<Sink>)> {
    let (count, sinks) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sinks WHERE parent_id = ?1 AND name LIKE ?2",
            )
            .bind(parent_id.to_string())
            .bind(format!("%{}%", name))
            .fetch_one(storage)
            .await?;

            let sinks = sqlx::query_as::<_, Sink>(
                "SELECT * FROM sinks WHERE parent_id = ?1 AND name LIKE ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            ).bind(parent_id.to_string())
            .bind(format!("%{}%", name))
            .fetch_all(storage).await?;

            (count, sinks)
        }
        None => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sinks WHERE parent_id = ?1")
                .bind(parent_id.to_string())
                .fetch_one(storage)
                .await?;
            let sinks = sqlx::query_as::<_, Sink>(
                "SELECT * FROM sinks WHERE parent_id = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(parent_id.to_string())
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count, sinks)
        }
    };

    Ok((count as usize, sinks))
}

pub async fn count_by_parent_id(storage: &AnyPool, parent_id: &Uuid) -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sinks WHERE parent_id = ?1")
        .bind(parent_id.to_string())
        .fetch_one(storage)
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

pub async fn check_delete_all(storage: &AnyPool, parent_id: &Uuid) -> Result<bool> {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sinks WHERE parent_id = ?1 AND rule_ref = 0")
            .bind(parent_id.to_string())
            .fetch_one(storage)
            .await?;

    Ok(count == 0)
}

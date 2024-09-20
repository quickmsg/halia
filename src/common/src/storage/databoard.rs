use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use types::{
    databoard::{CreateUpdateDataboardReq, QueryParams},
    Pagination,
};
use uuid::Uuid;

use super::databoard_data;

#[derive(FromRow)]
pub struct Databoard {
    pub id: String,
    pub status: i32,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS databoards (
    id TEXT PRIMARY KEY,
    status INTEGER NOT NULL,
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

pub async fn insert(storage: &AnyPool, id: &Uuid, req: CreateUpdateDataboardReq) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    let ts = chrono::Utc::now().timestamp();
    sqlx::query(
        "INSERT INTO databoards (id, status, name, desc, conf, ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .bind(id.to_string())
    .bind(false as i32)
    .bind(req.base.name)
    .bind(req.base.desc)
    .bind(conf)
    .bind(ts)
    .execute(storage)
    .await?;
    Ok(())
}

pub async fn query(
    storage: &AnyPool,
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Databoard>)> {
    let (count, databoards) = match (query_params.name, query_params.on) {
        (None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM databoards")
                .fetch_one(storage)
                .await?;

            let databoards = sqlx::query_as::<_, Databoard>(
                "SELECT * FROM databoards ORDER BY ts DESC LIMIT ?1 OFFSET ?2",
            )
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, databoards)
        }
        (None, Some(on)) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM databoards WHERE status = ?1")
                    .bind(on as i32)
                    .fetch_one(storage)
                    .await?;

            let databoards = sqlx::query_as::<_, Databoard>(
                "SELECT * FROM databoards WHERE status = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, databoards)
        }
        (Some(name), None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM databoards WHERE name = ?1")
                .bind(format!("%{}%", name))
                .fetch_one(storage)
                .await?;

            let databoards = sqlx::query_as::<_, Databoard>(
                "SELECT * FROM databoards WHERE name = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(format!("%{}%", name))
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, databoards)
        }
        (Some(name), Some(on)) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM databoards WHERE name = ?1 AND status = ?2",
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .fetch_one(storage)
            .await?;

            let databoards = sqlx::query_as::<_, Databoard>(
                "SELECT * FROM databoards WHERE name = ?1 AND status = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, databoards)
        }
    };

    Ok((count, databoards))
}

pub async fn read_one(storage: &AnyPool, id: &Uuid) -> Result<Databoard> {
    let databoard = sqlx::query_as::<_, Databoard>("SELECT * FROM databoards WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(storage)
        .await?;

    Ok(databoard)
}

pub async fn read_many_on(storage: &AnyPool) -> Result<Vec<Databoard>> {
    let databoards = sqlx::query_as::<_, Databoard>("SELECT * FROM databoards WHERE status = 1")
        .fetch_all(storage)
        .await?;

    Ok(databoards)
}

pub async fn count(storage: &AnyPool) -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM databoards")
        .fetch_one(storage)
        .await?;
    Ok(count as usize)
}

pub async fn update_conf(
    storage: &AnyPool,
    id: &Uuid,
    req: CreateUpdateDataboardReq,
) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    sqlx::query("UPDATE databoards SET name = ?1, desc = ?2, conf = ?1 WHERE id = ?2")
        .bind(req.base.name)
        .bind(req.base.desc)
        .bind(conf)
        .bind(id.to_string())
        .execute(storage)
        .await?;
    Ok(())
}

pub async fn update_status(storage: &AnyPool, id: &Uuid, status: bool) -> Result<()> {
    sqlx::query("UPDATE databoards SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id.to_string())
        .execute(storage)
        .await?;
    Ok(())
}

pub async fn delete(storage: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM databoards WHERE id = ?1")
        .bind(id.to_string())
        .execute(storage)
        .await?;

    databoard_data::delete_many(storage, id).await?;

    Ok(())
}

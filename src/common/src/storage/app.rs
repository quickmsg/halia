use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use types::{
    apps::{CreateUpdateAppReq, QueryParams},
    Pagination,
};
use uuid::Uuid;

#[derive(FromRow)]
pub struct App {
    pub id: String,
    pub status: i32,
    pub typ: String,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
    pub ts: i64,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS apps (
    id TEXT PRIMARY KEY,
    status INTEGER NOT NULL,
    typ TEXT NOT NULL,
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

pub async fn insert(storage: &AnyPool, id: &Uuid, req: CreateUpdateAppReq) -> Result<()> {
    let ts = chrono::Utc::now().timestamp();
    let conf = serde_json::to_string(&req.conf.ext)?;
    sqlx::query("INSERT INTO apps (id, status, typ, name, desc, conf, ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")
        .bind(id.to_string())
        .bind(false as i32)
        .bind(req.typ.to_string())
        .bind(req.conf.base.name)
        .bind(req.conf.base.desc)
        .bind(conf)
        .bind(ts)
        .execute(storage)
        .await?;

    Ok(())
}

pub async fn count(storage: &AnyPool) -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps")
        .fetch_one(storage)
        .await?;

    Ok(count as usize)
}

pub async fn read_one(storage: &AnyPool, id: &Uuid) -> Result<App> {
    let app = sqlx::query_as::<_, App>("SELECT * FROM apps WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(storage)
        .await?;

    Ok(app)
}

pub async fn read_on_all(storage: &AnyPool) -> Result<Vec<App>> {
    let apps = sqlx::query_as::<_, App>("SELECT * FROM apps WHERE status = 1")
        .fetch_all(storage)
        .await?;

    Ok(apps)
}

pub async fn query(
    storage: &AnyPool,
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<App>)> {
    let (count, apps) = match (query_params.name, query_params.typ, query_params.on) {
        (None, None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps")
                .fetch_one(storage)
                .await?;

            let apps =
                sqlx::query_as::<_, App>("SELECT * FROM apps ORDER BY ts DESC LIMIT ?1 OFFSET ?2")
                    .bind(pagination.size as i64)
                    .bind(((pagination.page - 1) * pagination.size) as i64)
                    .fetch_all(storage)
                    .await?;

            (count as usize, apps)
        }
        (None, None, Some(on)) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE status = ?1")
                .bind(on as i32)
                .fetch_one(storage)
                .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE status = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, apps)
        }
        (None, Some(typ), None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE typ = ?1")
                .bind(typ.to_string())
                .fetch_one(storage)
                .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE typ = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(typ.to_string())
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, apps)
        }
        (None, Some(typ), Some(on)) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE typ = ?1 AND status = ?2")
                    .bind(typ.to_string())
                    .bind(on as i32)
                    .fetch_one(storage)
                    .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE typ = ?1 AND status = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(typ.to_string())
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, apps)
        }
        (Some(name), None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE name = ?1")
                .bind(format!("%{}%", name))
                .fetch_one(storage)
                .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(format!("%{}%", name))
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, apps)
        }
        (Some(name), None, Some(on)) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE name = ?1 AND status = ?2")
                    .bind(format!("%{}%", name))
                    .bind(on as i32)
                    .fetch_one(storage)
                    .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name = ?1 AND status = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, apps)
        }
        (Some(name), Some(typ), None) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM apps WHERE name = ?1 AND typ = ?2",
            )
            .bind(format!("%{}%", name))
            .bind(typ.to_string())
            .fetch_one(storage)
            .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name = ?1 AND typ = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(format!("%{}%", name))
            .bind(typ.to_string())
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, apps)
        }
        (Some(name), Some(typ), Some(on)) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM apps WHERE name = ?1 AND typ = ?2 AND status = ?3",
            )
            .bind(format!("%{}%", name))
            .bind(typ.to_string())
            .bind(on as i32)
            .fetch_one(storage)
            .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name = ?1 AND typ = ?2 AND status = ?3 ORDER BY ts DESC LIMIT ?4 OFFSET ?5",
            )
            .bind(format!("%{}%", name))
            .bind(typ.to_string())
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, apps)
        }
    };

    Ok((count as usize, apps))
}

pub async fn update_status(storage: &AnyPool, id: &Uuid, status: bool) -> Result<()> {
    sqlx::query("UPDATE apps SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id.to_string())
        .execute(storage)
        .await?;

    Ok(())
}

pub async fn update(storage: &AnyPool, id: &Uuid, req: CreateUpdateAppReq) -> Result<()> {
    let conf = serde_json::to_string(&req.conf.ext)?;
    sqlx::query("UPDATE apps SET name = ?1, desc = ?2, conf = ?3 WHERE id = ?4")
        .bind(req.conf.base.name)
        .bind(req.conf.base.desc)
        .bind(conf)
        .bind(id.to_string())
        .execute(storage)
        .await?;
    Ok(())
}

pub async fn delete(storage: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM apps WHERE id = ?1")
        .bind(id.to_string())
        .execute(storage)
        .await?;

    Ok(())
}

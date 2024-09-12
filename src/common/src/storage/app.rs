use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use types::apps::CreateUpdateAppReq;
use uuid::Uuid;

#[derive(FromRow)]
pub struct App {
    pub id: String,
    pub status: i32,
    pub conf: String,
}

pub async fn insert(storage: &AnyPool, id: &Uuid, req: CreateUpdateAppReq) -> Result<()> {
    sqlx::query("INSERT INTO apps (id, status, conf) VALUES (?1, ?2, ?3)")
        .bind(id.to_string())
        .bind(false as i32)
        // .bind(conf)
        .execute(storage)
        .await?;

    Ok(())
}

pub async fn count_all(storage: &AnyPool) -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps")
        .fetch_one(storage)
        .await?;

    Ok(count as usize)
}

pub async fn read_on(storage: &AnyPool) -> Result<Vec<App>> {
    let apps = sqlx::query_as::<_, App>("SELECT * FROM apps WHERE status = 1")
        .fetch_all(storage)
        .await?;

    Ok(apps)
}

pub async fn update_status(pool: &AnyPool, id: &Uuid, status: bool) -> Result<()> {
    sqlx::query("UPDATE apps SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id.to_string())
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn update(pool: &AnyPool, id: &Uuid, req: CreateUpdateAppReq) -> Result<()> {
    sqlx::query("UPDATE apps SET conf = ?1 WHERE id = ?2")
        // .bind(conf)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query(
        r#"
DELETE FROM apps WHERE id = ?1
"#,
    )
    .bind(id.to_string())
    .execute(pool)
    .await?;

    Ok(())
}

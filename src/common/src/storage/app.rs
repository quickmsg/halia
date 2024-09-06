use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use uuid::Uuid;

#[derive(FromRow)]
pub struct App {
    pub id: String,
    pub status: i32,
    pub conf: String,
}

pub async fn create_app(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("INSERT INTO apps (id, status, conf) VALUES (?1, ?2, ?3)")
        .bind(id.to_string())
        .bind(false as i32)
        .bind(conf)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn read_apps(pool: &AnyPool) -> Result<Vec<App>> {
    let apps = sqlx::query_as::<_, App>("SELECT id, status, conf FROM apps")
        .fetch_all(pool)
        .await?;

    // let mut apps = vec![];
    // for row in rows {
    //     let id: String = row.get("id");
    //     let status: bool = row.get("bool");
    //     let conf: String = row.get("conf");
    //     apps.push(App { id, status, conf })
    // }

    Ok(apps)
}

pub async fn update_app_status(pool: &AnyPool, id: &Uuid, status: bool) -> Result<()> {
    sqlx::query("UPDATE apps SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id.to_string())
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn update_app_conf(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("UPDATE apps SET conf = ?1 WHERE id = ?2")
        .bind(conf)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_app(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query(
        r#"
DELETE FROM apps WHERE id = ?1;
DELETE FROM sources WHERE parent_id = ?1;
DELETE FROM sinks WHERE parent_id = ?1;
"#,
    )
    .bind(id.to_string())
    .execute(pool)
    .await?;

    Ok(())
}

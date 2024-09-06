use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use uuid::Uuid;

#[derive(FromRow)]
pub struct Device {
    pub id: String,
    pub status: i32,
    pub conf: String,
}

pub async fn create_device(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("INSERT INTO devices (id, status, conf) VALUES (?1, ?2, ?3)")
        .bind(id.to_string())
        .bind(false as i32)
        .bind(conf)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn read_devices(pool: &AnyPool) -> Result<Vec<Device>> {
    let devices = sqlx::query_as::<_, Device>("SELECT id, status, conf FROM devices")
        .fetch_all(pool)
        .await?;

    Ok(devices)
}

pub async fn update_device_status(pool: &AnyPool, id: &Uuid, status: bool) -> Result<()> {
    sqlx::query("UPDATE devices SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id.to_string())
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn update_device_conf(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("UPDATE devices SET conf = ?1 WHERE id = ?2")
        .bind(conf)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_device(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query(
        r#"
DELETE FROM devices WHERE id = ?1;
DELETE FROM sources WHERE parent_id = ?1;
DELETE FROM sinks WHERE parent_id = ?1;
    "#,
    )
    .bind(id.to_string())
    .execute(pool)
    .await?;

    Ok(())
}

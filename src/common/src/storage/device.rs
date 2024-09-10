use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use types::{
    devices::{CreateUpdateDeviceReq, QueryParams},
    Pagination,
};
use uuid::Uuid;

#[derive(FromRow, Debug)]
pub struct Device {
    pub id: String,
    pub status: i32,
    pub device_type: String,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
}

#[derive(FromRow)]
pub struct Event {
    pub id: String,
    pub event_type: i32,
    pub ts: i64,
    pub info: Option<String>,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS devices (
    id TEXT PRIMARY KEY,
    status INTEGER NOT NULL,
    device_type TEXT NOT NULL,
    name TEXT NOT NULL,
    desc TEXT,
    conf TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device_events (
    id TEXT,
    event_type INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    info TEXT
);
"#,
    )
    .execute(storage)
    .await?;

    Ok(())
}

pub async fn create_device(pool: &AnyPool, id: &Uuid, req: CreateUpdateDeviceReq) -> Result<()> {
    let ext_conf = serde_json::to_string(&req.conf.ext)?;
    match req.conf.base.desc {
        Some(desc) => {
            sqlx::query("INSERT INTO devices (id, status, device_type, name, desc, conf) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")
                .bind(id.to_string())
                .bind(false as i32)
                .bind(req.device_type.to_string())
                .bind(req.conf.base.name)
                .bind(desc)
                .bind(ext_conf)
                .execute(pool)
                .await?;
        }
        None => {
            sqlx::query(
                "INSERT INTO devices (id, status, device_type, name, conf) VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(id.to_string())
            .bind(false as i32)
            .bind(req.device_type.to_string())
            .bind(req.conf.base.name)
            .bind(ext_conf)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}

pub async fn read_devices(pool: &AnyPool) -> Result<Vec<Device>> {
    let devices = sqlx::query_as::<_, Device>("SELECT * FROM devices")
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

// 考虑，是否删除该设备的事件
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

pub async fn create_event(
    storage: &AnyPool,
    id: &Uuid,
    event_type: i32,
    info: Option<String>,
) -> Result<()> {
    let ts = chrono::Utc::now().timestamp();
    match info {
        Some(info) => {
            sqlx::query(
                "INSERT INTO events (id, source_type, event_type, ts, info) VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(id.to_string())
            .bind(event_type)
            .bind(ts)
            .bind(info)
            .execute(storage)
            .await?;
        }
        None => {
            sqlx::query(
                "INSERT INTO events (id, source_type, event_type, ts) VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(id.to_string())
            .bind(event_type)
            .bind(ts)
            .execute(storage)
            .await?;
        }
    }

    Ok(())
}

pub async fn search_events(
    storage: &AnyPool,
    query_params: QueryParams,
    pagination: Pagination,
) -> Result<(Vec<Event>, i64)> {
    // match query_params.event_type {
    //     Some(_) => todo!(),
    //     None => todo!(),
    // }
    let offset = (pagination.page - 1) * pagination.size;
    // 设备源
    let events = sqlx::query_as::<_, Event>(
        r#"
SELECT events.* FROM events 
INNERT JOIN devices ON events.id == devices.id
ORDER BY evetns.ts DESC LIMIT ? OFFSET ?"#,
    )
    .bind(pagination.size as i64)
    .bind(offset as i64)
    .fetch_all(storage)
    .await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(storage)
        .await?;

    Ok((events, count))
}

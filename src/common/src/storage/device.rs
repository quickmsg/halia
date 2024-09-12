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
    conf TEXT NOT NULL,
    ts INT NOT NULL
);

CREATE TABLE IF NOT EXISTS device_events (
    id TEXT,
    event_type INTEGER NOT NULL,
    info TEXT,
    ts INTEGER NOT NULL
);
"#,
    )
    .execute(storage)
    .await?;

    Ok(())
}

pub async fn create_device(pool: &AnyPool, id: &Uuid, req: CreateUpdateDeviceReq) -> Result<()> {
    let ext_conf = serde_json::to_string(&req.conf.ext)?;
    let ts = chrono::Utc::now().timestamp();
    match req.conf.base.desc {
        Some(desc) => {
            sqlx::query("INSERT INTO devices (id, status, device_type, name, desc, conf, ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")
                .bind(id.to_string())
                .bind(false as i32)
                .bind(req.device_type.to_string())
                .bind(req.conf.base.name)
                .bind(desc)
                .bind(ext_conf)
                .bind(ts)
                .execute(pool)
                .await?;
        }
        None => {
            sqlx::query(
                "INSERT INTO devices (id, status, device_type, name, conf, ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .bind(id.to_string())
            .bind(false as i32)
            .bind(req.device_type.to_string())
            .bind(req.conf.base.name)
            .bind(ext_conf)
            .bind(ts)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}

pub async fn read_device(storage: &AnyPool, id: &Uuid) -> Result<Device> {
    let device = sqlx::query_as::<_, Device>("SELECT * FROM devices WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(storage)
        .await?;

    Ok(device)
}

pub async fn search_devices(
    storage: &AnyPool,
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Device>)> {
    let (count, devices) = match (query_params.name, query_params.device_type, query_params.on) {
        (None, None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
                .fetch_one(storage)
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices ORDER BY ts DESC LIMIT ?1 OFFSET ?2",
            )
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, devices)
        }
        (None, None, Some(on)) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE status = ?1")
                .bind(on as i32)
                .fetch_one(storage)
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE status = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, devices)
        }
        (None, Some(device_type), None) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE device_type = ?1")
                    .bind(device_type.to_string())
                    .fetch_one(storage)
                    .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE device_type = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(device_type.to_string())
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, devices)
        }
        (None, Some(device_type), Some(on)) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM devices WHERE device_type = ?1 AND status = ?2",
            )
            .bind(device_type.to_string())
            .bind(on as i32)
            .fetch_one(storage)
            .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE device_type = ?1 AND status = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(device_type.to_string())
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, devices)
        }
        (Some(name), None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ?1")
                .bind(format!("%{}%", name))
                .fetch_one(storage)
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(format!("%{}%", name))
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, devices)
        }
        (Some(name), None, Some(on)) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ?1 AND status = ?2")
                    .bind(format!("%{}%", name))
                    .bind(on as i32)
                    .fetch_one(storage)
                    .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name = ?1 AND status = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, devices)
        }
        (Some(name), Some(device_type), None) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM devices WHERE name = ?1 AND device_type = ?2",
            )
            .bind(format!("%{}%", name))
            .bind(device_type.to_string())
            .fetch_one(storage)
            .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name = ?1 AND device_type = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(format!("%{}%", name))
            .bind(device_type.to_string())
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, devices)
        }
        (Some(name), Some(device_type), Some(on)) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM devices WHERE name = ?1 AND device_type = ?2 AND status = ?3",
            )
            .bind(format!("%{}%", name))
            .bind(device_type.to_string())
            .bind(on as i32)
            .fetch_one(storage)
            .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name = ?1 AND device_type = ?2 AND status = ?3 ORDER BY ts DESC LIMIT ?4 OFFSET ?5",
            )
            .bind(format!("%{}%", name))
            .bind(device_type.to_string())
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count as usize, devices)
        }
    };

    Ok((count as usize, devices))
}

pub async fn read_on(storage: &AnyPool) -> Result<Vec<Device>> {
    let devices = sqlx::query_as::<_, Device>("SELECT * FROM devices WHERE status = 1")
        .fetch_all(storage)
        .await?;

    Ok(devices)
}

pub async fn count_all(storage: &AnyPool) -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
        .fetch_one(storage)
        .await?;

    Ok(count as usize)
}

pub async fn update_device_status(pool: &AnyPool, id: &Uuid, status: bool) -> Result<()> {
    sqlx::query("UPDATE devices SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id.to_string())
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn update_device_conf(
    pool: &AnyPool,
    id: &Uuid,
    req: CreateUpdateDeviceReq,
) -> Result<()> {
    let ext_conf = serde_json::to_string(&req.conf.ext)?;
    sqlx::query("UPDATE devices SET name = ?1, desc = ?2, conf = ?3 WHERE id = ?4")
        .bind(req.conf.base.name)
        .bind(req.conf.base.desc)
        .bind(ext_conf)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

// 考虑，是否删除该设备的事件
pub async fn delete_device(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query(
        "DELETE FROM devices WHERE id = ?1
    ",
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
                "INSERT INTO device_events (id, event_type, ts, info) VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(id.to_string())
            .bind(event_type)
            .bind(ts)
            .bind(info)
            .execute(storage)
            .await?;
        }
        None => {
            sqlx::query("INSERT INTO device_events (id, event_type, ts) VALUES (?1, ?2, ?3)")
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

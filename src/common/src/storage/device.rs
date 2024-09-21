use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use types::{
    devices::{CreateUpdateDeviceReq, QueryParams},
    Pagination,
};

use super::POOL;

#[derive(FromRow, Debug)]
pub struct Device {
    pub id: String,
    pub status: i32,
    pub typ: String,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
    pub ts: i64,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS devices (
    id VARCHAR(255) PRIMARY KEY,     -- 对于 MySQL, VARCHAR 是推荐的字符串类型
    status INTEGER NOT NULL,         -- INTEGER 适用于两者
    typ VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    `desc` TEXT,                     -- `desc` 是保留字，使用反引号避免冲突
    conf TEXT NOT NULL,
    ts BIGINT NOT NULL               -- 时间戳使用 BIGINT 以保证两者兼容
);
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn insert_name_exists(name: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ?1")
        .bind(name)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count > 0)
}

pub async fn update_name_exists(id: &String, name: &String) -> Result<bool> {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ?1 AND id != ?2")
            .bind(name)
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(count > 0)
}

pub async fn insert(id: &String, req: CreateUpdateDeviceReq) -> Result<()> {
    let ext_conf = serde_json::to_string(&req.conf.ext)?;
    let ts = chrono::Utc::now().timestamp();
    match req.conf.base.desc {
        Some(desc) => {
            sqlx::query("INSERT INTO devices (id, status, typ, name, desc, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?)")
                .bind(id)
                .bind(false as i32)
                .bind(req.device_type.to_string())
                .bind(req.conf.base.name)
                .bind(desc)
                .bind(ext_conf)
                .bind(ts)
                .execute(POOL.get().unwrap())
                .await?;
        }
        None => {
            sqlx::query(
                "INSERT INTO devices (id, status, typ, name, conf, ts) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(false as i32)
            .bind(req.device_type.to_string())
            .bind(req.conf.base.name)
            .bind(ext_conf)
            .bind(ts)
            .execute(POOL.get().unwrap())
            .await?;
        }
    }

    Ok(())
}

pub async fn read_device(id: &String) -> Result<Device> {
    let device = sqlx::query_as::<_, Device>("SELECT * FROM devices WHERE id = ?1")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(device)
}

pub async fn search_devices(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Device>)> {
    let (count, devices) = match (query_params.name, query_params.device_type, query_params.on) {
        (None, None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
                .fetch_one(POOL.get().unwrap())
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices ORDER BY ts DESC LIMIT ?1 OFFSET ?2",
            )
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (None, None, Some(on)) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE status = ?1")
                .bind(on as i32)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE status = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (None, Some(device_type), None) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE device_type = ?1")
                    .bind(device_type.to_string())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE device_type = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(device_type.to_string())
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (None, Some(device_type), Some(on)) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM devices WHERE device_type = ?1 AND status = ?2",
            )
            .bind(device_type.to_string())
            .bind(on as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE device_type = ?1 AND status = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(device_type.to_string())
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (Some(name), None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ?1")
                .bind(format!("%{}%", name))
                .fetch_one(POOL.get().unwrap())
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(format!("%{}%", name))
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (Some(name), None, Some(on)) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ?1 AND status = ?2")
                    .bind(format!("%{}%", name))
                    .bind(on as i32)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name = ?1 AND status = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (Some(name), Some(device_type), None) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM devices WHERE name = ?1 AND device_type = ?2",
            )
            .bind(format!("%{}%", name))
            .bind(device_type.to_string())
            .fetch_one(POOL.get().unwrap())
            .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name = ?1 AND device_type = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(format!("%{}%", name))
            .bind(device_type.to_string())
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap())
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
            .fetch_one(POOL.get().unwrap())
            .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name = ?1 AND device_type = ?2 AND status = ?3 ORDER BY ts DESC LIMIT ?4 OFFSET ?5",
            )
            .bind(format!("%{}%", name))
            .bind(device_type.to_string())
            .bind(on as i32)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
    };

    Ok((count as usize, devices))
}

pub async fn read_on() -> Result<Vec<Device>> {
    let devices = sqlx::query_as::<_, Device>("SELECT * FROM devices WHERE status = 1")
        .fetch_all(POOL.get().unwrap())
        .await?;

    Ok(devices)
}

pub async fn count_all() -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

pub async fn update_status(id: &String, status: bool) -> Result<()> {
    sqlx::query("UPDATE devices SET status = ?1 WHERE id = ?2")
        .bind(status as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn update(id: &String, req: CreateUpdateDeviceReq) -> Result<()> {
    let ext_conf = serde_json::to_string(&req.conf.ext)?;
    sqlx::query("UPDATE devices SET name = ?1, desc = ?2, conf = ?3 WHERE id = ?4")
        .bind(req.conf.base.name)
        .bind(req.conf.base.desc)
        .bind(ext_conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete(id: &String) -> Result<()> {
    sqlx::query(
        "DELETE FROM devices WHERE id = ?1
    ",
    )
    .bind(id)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

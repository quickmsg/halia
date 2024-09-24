use anyhow::Result;
use sqlx::prelude::FromRow;
use types::{
    devices::{CreateUpdateDeviceReq, QueryParams},
    Pagination,
};

use super::POOL;

#[derive(FromRow)]
pub struct Device {
    pub id: String,
    pub status: i32,
    pub typ: i32,
    pub name: String,
    // desc为关键字
    pub des: Option<Vec<u8>>,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS devices (
    id CHAR(32) PRIMARY KEY,
    status SMALLINT UNSIGNED NOT NULL,
    typ SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn insert_name_exists(name: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ?")
        .bind(name)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count > 0)
}

pub async fn update_name_exists(id: &String, name: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ? AND id != ?")
        .bind(name)
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count > 0)
}

pub async fn insert(id: &String, req: CreateUpdateDeviceReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.conf.ext)?;
    let ts = chrono::Utc::now().timestamp();
    let typ: i32 = req.typ.into();
    let desc = req.conf.base.desc.map(|desc| desc.into_bytes());
    sqlx::query(
        "INSERT INTO devices (id, status, typ, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(false as i32)
    .bind(typ)
    .bind(req.conf.base.name)
    .bind(desc)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_device(id: &String) -> Result<Device> {
    let device = sqlx::query_as::<_, Device>("SELECT * FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(device)
}

pub async fn search_devices(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Device>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, devices) = match (query_params.name, query_params.typ, query_params.on) {
        (None, None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
                .fetch_one(POOL.get().unwrap())
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (None, None, Some(on)) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE status = ?")
                .bind(on as i32)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (None, Some(device_type), None) => {
            let typ: i32 = device_type.into();
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE typ = ?")
                .bind(typ)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE typ = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(typ)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (None, Some(device_type), Some(on)) => {
            let typ: i32 = device_type.into();
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE typ = ? AND status = ?")
                    .bind(typ)
                    .bind(on as i32)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE typ = ? AND status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(typ)
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (Some(name), None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name LIKE ?")
                .bind(format!("%{}%", name))
                .fetch_one(POOL.get().unwrap())
                .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (Some(name), None, Some(on)) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name LIKE ? AND status = ?")
                    .bind(format!("%{}%", name))
                    .bind(on as i32)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name LIKE ? AND status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (Some(name), Some(device_type), None) => {
            let typ: i32 = device_type.into();
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM devices WHERE name LIKE ? AND device_type = ?",
            )
            .bind(format!("%{}%", name))
            .bind(typ)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name LIKE ? AND device_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(typ)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, devices)
        }
        (Some(name), Some(device_type), Some(on)) => {
            let typ: i32 = device_type.into();
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM devices WHERE name LIKE ? AND typ = ? AND status = ?",
            )
            .bind(format!("%{}%", name))
            .bind(typ)
            .bind(on as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let devices = sqlx::query_as::<_, Device>(
                "SELECT * FROM devices WHERE name LIKE ? AND typ = ? AND status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(typ)
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
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

pub async fn read_name(id: &String) -> Result<String> {
    let name: String = sqlx::query_scalar("SELECT name FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(name)
}

pub async fn read_type(id: &String) -> Result<i32> {
    let typ: i32 = sqlx::query_scalar("SELECT typ FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(typ)
}

pub async fn count_all() -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

pub async fn update_status(id: &String, status: bool) -> Result<()> {
    sqlx::query("UPDATE devices SET status = ? WHERE id = ?")
        .bind(status as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn update(id: &String, req: CreateUpdateDeviceReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.conf.ext)?;
    let desc = req.conf.base.desc.map(|desc| desc.into_bytes());
    sqlx::query("UPDATE devices SET name = ?, des = ?, conf = ? WHERE id = ?")
        .bind(req.conf.base.name)
        .bind(desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete(id: &String) -> Result<()> {
    sqlx::query("DELETE FROM devices WHERE id = ?")
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

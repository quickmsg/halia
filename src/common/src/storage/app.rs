use anyhow::Result;
use sqlx::prelude::FromRow;
use types::{
    apps::{CreateUpdateAppReq, QueryParams},
    Pagination,
};

use super::POOL;

#[derive(FromRow)]
pub struct App {
    pub id: String,
    pub status: i32,
    pub err: i32,
    pub typ: i32,
    pub name: String,
    pub des: Option<Vec<u8>>,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS apps (
    id CHAR(32) PRIMARY KEY,
    status SMALLINT UNSIGNED NOT NULL,
    err SMALLINT UNSIGNED NOT NULL,
    typ SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
)
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn insert_name_exists(name: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE name = ?")
        .bind(name)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count > 0)
}

pub async fn update_name_exists(id: &String, name: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE name = ? AND id != ?")
        .bind(name)
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count > 0)
}

pub async fn insert(id: &String, req: CreateUpdateAppReq) -> Result<()> {
    let typ: i32 = req.typ.into();
    let ts = chrono::Utc::now().timestamp();
    let conf = serde_json::to_vec(&req.conf.ext)?;
    let desc = req.conf.base.desc.map(|desc| desc.into_bytes());
    sqlx::query(
        "INSERT INTO apps (id, status, err, typ, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(false as i32)
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

pub async fn count() -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps")
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

pub async fn read_name(id: &String) -> Result<String> {
    let name: String = sqlx::query_scalar("SELECT name FROM apps WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(name)
}

pub async fn read_conf(id: &String) -> Result<Vec<u8>> {
    let conf: Vec<u8> = sqlx::query_scalar("SELECT conf FROM apps WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(conf)
}

pub async fn read_type(id: &String) -> Result<i32> {
    let typ: i32 = sqlx::query_scalar("SELECT typ FROM apps WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(typ)
}

pub async fn read_one(id: &String) -> Result<App> {
    let app = sqlx::query_as::<_, App>("SELECT * FROM apps WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(app)
}

pub async fn read_on_all() -> Result<Vec<App>> {
    let apps = sqlx::query_as::<_, App>("SELECT * FROM apps WHERE status = 1")
        .fetch_all(POOL.get().unwrap())
        .await?;

    Ok(apps)
}

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<App>)> {
    let limit = pagination.size as i64;
    let offset = ((pagination.page - 1) * pagination.size) as i64;
    let (count, apps) = match (
        query_params.name,
        query_params.typ,
        query_params.on,
        query_params.err,
    ) {
        (None, None, None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps")
                .fetch_one(POOL.get().unwrap())
                .await?;

            let apps =
                sqlx::query_as::<_, App>("SELECT * FROM apps ORDER BY ts DESC LIMIT ? OFFSET ?")
                    .bind(limit)
                    .bind(offset)
                    .fetch_all(POOL.get().unwrap())
                    .await?;

            (count as usize, apps)
        }
        (None, None, None, Some(err)) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE err = ?")
                .bind(err as i32)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE err = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(err as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (None, None, Some(on), None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE status = ?")
                .bind(on as i32)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (None, None, Some(on), Some(err)) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE status = ? AND err = ?")
                    .bind(on as i32)
                    .bind(err as i32)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE status = ? AND err = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(on as i32)
            .bind(err as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (None, Some(typ), None, None) => {
            let typ: i32 = typ.into();
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE typ = ?")
                .bind(typ.to_string())
                .fetch_one(POOL.get().unwrap())
                .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE typ = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(typ)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (None, Some(typ), None, Some(err)) => {
            let typ: i32 = typ.into();
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE typ = ? AND err = ?")
                    .bind(typ)
                    .bind(err as i32)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE typ = ? AND err = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(typ)
            .bind(err as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (None, Some(typ), Some(on), None) => {
            let typ: i32 = typ.into();
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE typ = ? AND status = ?")
                    .bind(typ)
                    .bind(on as i32)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE typ = ? AND status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(typ)
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (None, Some(typ), Some(on), Some(err)) => {
            let typ: i32 = typ.into();
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM apps WHERE typ = ? AND status = ? AND err = ?",
            )
            .bind(typ)
            .bind(on as i32)
            .bind(err as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE typ = ? AND status = ? AND err = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(typ)
            .bind(on as i32)
            .bind(err as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (Some(name), None, None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE name LIKE ?")
                .bind(format!("%{}%", name))
                .fetch_one(POOL.get().unwrap())
                .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (Some(name), None, None, Some(err)) => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE name LIKE ? AND err = ?")
                    .bind(format!("%{}%", name))
                    .bind(err as i32)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name LIKE ? AND err = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(format!("%{}%", name))
            .bind(err as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (Some(name), None, Some(on), None) => {
            let name = format!("%{}%", name);
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE name LIKE ? AND status = ?")
                    .bind(&name)
                    .bind(on as i32)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let apps = sqlx::query_as::<_, App>(
                    "SELECT * FROM apps WHERE name LIKE ? AND status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                )
                .bind(&name)
                .bind(on as i32)
                .bind(limit)
                .bind(offset)
                .fetch_all(POOL.get().unwrap())
                .await?;

            (count as usize, apps)
        }
        (Some(name), None, Some(on), Some(err)) => {
            let name = format!("%{}%", name);
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM apps WHERE name LIKE ? AND status = ? AND err = ?",
            )
            .bind(&name)
            .bind(on as i32)
            .bind(err as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name LIKE ? AND status = ? AND err = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(on as i32)
            .bind(err as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (Some(name), Some(typ), None, None) => {
            let name = format!("%{}%", name);
            let typ: i32 = typ.into();
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM apps WHERE name LIKE ? AND typ = ?")
                    .bind(&name)
                    .bind(typ)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name LIKE ? AND typ = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (Some(name), Some(typ), None, Some(err)) => {
            let name = format!("%{}%", name);
            let typ: i32 = typ.into();
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM apps WHERE name LIKE ? AND typ = ? AND err = ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(err as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name LIKE ? AND typ = ? AND err = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(err as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (Some(name), Some(typ), Some(on), None) => {
            let name = format!("%{}%", name);
            let typ: i32 = typ.into();
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM apps WHERE name LIKE ? AND typ = ? AND status = ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(on as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name LIKE ? AND typ = ? AND status = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(on as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
        (Some(name), Some(typ), Some(on), Some(err)) => {
            let name = format!("%{}%", name);
            let typ: i32 = typ.into();
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM apps WHERE name LIKE ? AND typ = ? AND status = ? AND err = ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(on as i32)
            .bind(err as i32)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let apps = sqlx::query_as::<_, App>(
                "SELECT * FROM apps WHERE name LIKE ? AND typ = ? AND status = ? AND err = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(on as i32)
            .bind(err as i32)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, apps)
        }
    };

    Ok((count, apps))
}

pub async fn update_status(id: &String, status: bool) -> Result<()> {
    sqlx::query("UPDATE apps SET status = ? WHERE id = ?")
        .bind(status as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn update_err(id: &String, err: bool) -> Result<()> {
    sqlx::query("UPDATE apps SET err = ? WHERE id = ?")
        .bind(err as i32)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn update(id: String, req: CreateUpdateAppReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.conf.ext)?;
    let desc = req.conf.base.desc.map(|desc| desc.into_bytes());
    sqlx::query("UPDATE apps SET name = ?, des = ?, conf = ? WHERE id = ?")
        .bind(req.conf.base.name)
        .bind(desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete(id: &String) -> Result<()> {
    sqlx::query("DELETE FROM apps WHERE id = ?")
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

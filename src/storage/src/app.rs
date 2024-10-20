use anyhow::{bail, Result};
use common::error::HaliaResult;
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    apps::{CreateUpdateAppReq, QueryParams},
    Pagination,
};

use super::POOL;

static TABLE_NAME: &str = "apps";

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

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    status SMALLINT UNSIGNED NOT NULL,
    err SMALLINT UNSIGNED NOT NULL,
    typ SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateUpdateAppReq) -> HaliaResult<()> {
    let typ: i32 = req.typ.into();
    let ts = common::timestamp_millis();
    let conf = serde_json::to_vec(&req.conf.ext)?;
    let desc = req.conf.base.desc.map(|desc| desc.into_bytes());

    sqlx::query("INSERT INTO apps (id, status, err, typ, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
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
    let (limit, offset) = pagination.to_sql();

    let (count, apps) = match (
        &query_params.name,
        &query_params.typ,
        &query_params.on,
        &query_params.err,
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

            (count, apps)
        }
        _ => {
            let mut where_clause = String::new();
            if query_params.name.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE name LIKE ?"),
                    false => where_clause.push_str(" AND name LIKE ?"),
                }
            }
            if query_params.typ.is_some() {
                let typ = transfer_type(&query_params.typ.unwrap())?;
                match where_clause.is_empty() {
                    true => where_clause.push_str(format!("WHERE {}", typ).as_str()),
                    false => where_clause.push_str(format!(" AND {}", typ).as_str()),
                }
            }
            if query_params.on.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE status = ?"),
                    false => where_clause.push_str(" AND status = ?"),
                }
            }
            if query_params.err.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE err = ?"),
                    false => where_clause.push_str(" AND err = ?"),
                }
            }

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(&query_count_str);

            let query_schemas_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );
            let mut query_schemas_builder: QueryAs<'_, Any, App, AnyArguments> =
                sqlx::query_as::<_, App>(&query_schemas_str);

            if let Some(name) = query_params.name {
                let name = format!("%{}%", name);
                query_count_builder = query_count_builder.bind(name.clone());
                query_schemas_builder = query_schemas_builder.bind(name);
            }
            if let Some(on) = query_params.on {
                query_count_builder = query_count_builder.bind(on as i32);
                query_schemas_builder = query_schemas_builder.bind(on as i32);
            }
            if let Some(err) = query_params.err {
                query_count_builder = query_count_builder.bind(err as i32);
                query_schemas_builder = query_schemas_builder.bind(err as i32);
            }

            let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
            let devices = query_schemas_builder
                .bind(limit)
                .bind(offset)
                .fetch_all(POOL.get().unwrap())
                .await?;

            (count, devices)
        }
    };

    Ok((count as usize, apps))
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

pub async fn update_conf(id: String, req: CreateUpdateAppReq) -> HaliaResult<()> {
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

fn transfer_type(typ: &str) -> Result<String> {
    let typ = match typ {
        "mqtt" => "(typ = 10 OR typ = 11)".to_owned(),
        "http" => "typ = 2".to_owned(),
        "kafka" => "typ = 3".to_owned(),
        "influxdb" => "(typ = 40 OR typ = 41)".to_owned(),
        "tdengine" => "typ = 5".to_owned(),
        _ => bail!("未知应用类型。"),
    };
    Ok(typ)
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    super::delete_by_id(id, TABLE_NAME).await
}

use anyhow::{bail, Result};
use common::error::HaliaResult;
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    apps::{AppType, CreateAppReq, QueryParams, UpdateAppReq},
    Pagination, Status,
};

use super::POOL;

pub mod source_sink;

static TABLE_NAME: &str = "apps";

#[derive(FromRow)]
struct DbApp {
    pub id: String,
    pub app_type: i32,
    pub name: String,
    pub conf: Vec<u8>,
    pub status: i32,
    pub ts: i64,
}

impl DbApp {
    pub fn transfer(self) -> Result<App> {
        Ok(App {
            id: self.id,
            status: self.status.try_into()?,
            app_type: self.app_type.try_into()?,
            name: self.name,
            conf: serde_json::from_slice(&self.conf)?,
            ts: self.ts,
        })
    }
}

pub struct App {
    pub id: String,
    pub app_type: AppType,
    pub name: String,
    pub conf: serde_json::Value,
    pub status: Status,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    app_type SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    conf BLOB NOT NULL,
    status SMALLINT UNSIGNED NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateAppReq) -> HaliaResult<()> {
    sqlx::query(
        format!(
            "INSERT INTO {} (id, app_type, name, conf, status, ts) VALUES (?, ?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(Into::<i32>::into(req.app_type))
    .bind(req.name)
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(Into::<i32>::into(Status::default()))
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn count() -> Result<usize> {
    let count: i64 = sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

pub async fn read_name(id: &String) -> Result<String> {
    let name: String =
        sqlx::query_scalar(format!("SELECT name FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(name)
}

pub async fn read_conf(id: &String) -> Result<serde_json::Value> {
    let conf: Vec<u8> =
        sqlx::query_scalar(format!("SELECT conf FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    let conf = serde_json::from_slice(&conf)?;
    Ok(conf)
}

pub async fn read_app_type(id: &String) -> Result<AppType> {
    let app_type: i32 =
        sqlx::query_scalar(format!("SELECT app_type FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    let app_type: AppType = app_type.try_into()?;

    Ok(app_type)
}

pub async fn read_one(id: &String) -> Result<App> {
    let db_app =
        sqlx::query_as::<_, DbApp>(format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    db_app.transfer()
}

pub async fn read_all_running() -> Result<Vec<App>> {
    let db_apps = sqlx::query_as::<_, DbApp>(
        format!("SELECT * FROM {} WHERE status = ?", TABLE_NAME).as_str(),
    )
    .bind(Into::<i32>::into(Status::Running))
    .fetch_all(POOL.get().unwrap())
    .await?;

    db_apps.into_iter().map(|x| x.transfer()).collect()
}

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<App>)> {
    let (limit, offset) = pagination.to_sql();

    let (count, db_apps) = match (&query_params.name, &query_params.typ, &query_params.status) {
        (None, None, None) => {
            let count: i64 =
                sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let apps = sqlx::query_as::<_, DbApp>(
                format!(
                    "SELECT * FROM {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
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
            if query_params.status.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE status = ?"),
                    false => where_clause.push_str(" AND status = ?"),
                }
            }

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(&query_count_str);

            let query_schemas_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );
            let mut query_schemas_builder: QueryAs<'_, Any, DbApp, AnyArguments> =
                sqlx::query_as::<_, DbApp>(&query_schemas_str);

            if let Some(name) = query_params.name {
                let name = format!("%{}%", name);
                query_count_builder = query_count_builder.bind(name.clone());
                query_schemas_builder = query_schemas_builder.bind(name);
            }
            if let Some(status) = query_params.status {
                let status: i32 = status.into();
                query_count_builder = query_count_builder.bind(status);
                query_schemas_builder = query_schemas_builder.bind(status);
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

    let apps = db_apps
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<_>>>()?;

    Ok((count as usize, apps))
}

pub async fn update_status(id: &String, status: Status) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET status = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(Into::<i32>::into(status))
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn update_conf(id: String, req: UpdateAppReq) -> HaliaResult<()> {
    sqlx::query(format!("UPDATE {} SET name = ?, conf = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(serde_json::to_vec(&req.conf)?)
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

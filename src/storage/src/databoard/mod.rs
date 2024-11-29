use anyhow::Result;
use sqlx::{any::AnyArguments, prelude::FromRow, query::QueryScalar, Any};
use types::{
    databoard::{CreateUpdateDataboardReq, QueryParams},
    Pagination, Status,
};

pub mod data;

use super::POOL;

static TABLE_NAME: &str = "databoards";

#[derive(FromRow)]
struct DbDataboard {
    pub id: String,
    pub status: i32,
    pub name: String,
    pub ts: i64,
}

impl DbDataboard {
    pub fn transfer(self) -> Result<Databoard> {
        Ok(Databoard {
            id: self.id,
            status: self.status.try_into()?,
            name: self.name,
            ts: self.ts,
        })
    }
}

pub struct Databoard {
    pub id: String,
    pub status: Status,
    pub name: String,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    status SMALLINT UNSIGNED NOT NULL,
    name VARCHAR(255) NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateUpdateDataboardReq) -> Result<()> {
    sqlx::query(
        format!(
            "INSERT INTO {} (id, status, name, ts) VALUES (?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(Into::<i32>::into(Status::default()))
    .bind(req.name)
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;
    Ok(())
}

pub async fn query(pagination: Pagination, query: QueryParams) -> Result<(usize, Vec<Databoard>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, db_databoards) = match (&query.name, &query.status) {
        (None, None) => {
            let count: i64 =
                sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let databoards = sqlx::query_as::<_, DbDataboard>(
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

            (count, databoards)
        }
        _ => {
            let mut where_clause = String::new();
            if query.name.is_some() {
                where_clause.push_str("WHERE name LIKE ?");
            }
            if query.status.is_some() {
                match where_clause.is_empty() {
                    true => where_clause.push_str("WHERE status = ?"),
                    false => where_clause.push_str(" AND status = ?"),
                }
            }

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(query_count_str.as_str());

            let query_schemas_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );
            let mut query_schemas_builder: sqlx::query::QueryAs<
                '_,
                Any,
                DbDataboard,
                AnyArguments,
            > = sqlx::query_as::<_, DbDataboard>(query_schemas_str.as_str());

            if let Some(name) = query.name {
                query_count_builder = query_count_builder.bind(format!("%{}%", name));
                query_schemas_builder = query_schemas_builder.bind(format!("%{}%", name));
            }
            if let Some(status) = query.status {
                query_count_builder = query_count_builder.bind(Into::<i32>::into(status));
                query_schemas_builder = query_schemas_builder.bind(Into::<i32>::into(status));
            }

            let count: i64 = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
            let databoards = query_schemas_builder
                .bind(limit)
                .bind(offset)
                .fetch_all(POOL.get().unwrap())
                .await?;

            (count, databoards)
        }
    };

    let databoards = db_databoards
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<_>>>()?;

    Ok((count as usize, databoards))
}

pub async fn read_one(id: &String) -> Result<Databoard> {
    let db_databoard = sqlx::query_as::<_, DbDataboard>(
        format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str(),
    )
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    db_databoard.transfer()
}

pub async fn read_name(id: &String) -> Result<String> {
    let name: String =
        sqlx::query_scalar(format!("SELECT name FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(name)
}

pub async fn read_all_running() -> Result<Vec<Databoard>> {
    let db_databoards = sqlx::query_as::<_, DbDataboard>(
        format!("SELECT * FROM {} WHERE status = ?", TABLE_NAME).as_str(),
    )
    .bind(Into::<i32>::into(Status::Running))
    .fetch_all(POOL.get().unwrap())
    .await?;

    db_databoards.into_iter().map(|x| x.transfer()).collect()
}

pub async fn count() -> Result<usize> {
    let count: i64 = sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
        .fetch_one(POOL.get().unwrap())
        .await?;
    Ok(count as usize)
}

pub async fn get_summary() -> Result<(usize, usize)> {
    let (total, running_cnt, _) = crate::get_summary(TABLE_NAME).await?;
    Ok((total as usize, running_cnt as usize))
}

pub async fn update_conf(id: &String, req: CreateUpdateDataboardReq) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET name = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn update_status(id: &String, status: Status) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET status = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(Into::<i32>::into(status))
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete_by_id(id: &String) -> Result<()> {
    sqlx::query(format!("DELETE FROM {} WHERE id = ?", TABLE_NAME).as_str())
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    data::delete_many(id).await?;

    Ok(())
}

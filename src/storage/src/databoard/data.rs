use anyhow::Result;
use common::error::HaliaResult;
use sqlx::FromRow;
use types::{
    databoard::{CreateUpdateDataReq, DataConf, QueryDatasParams},
    Pagination,
};

use super::POOL;

static TABLE_NAME: &str = "databoard_datas";

#[derive(FromRow)]
struct DbData {
    pub id: String,
    pub databoard_id: String,
    pub name: String,
    pub conf: Vec<u8>,
    pub ts: i64,
}

impl DbData {
    pub fn transfer(self) -> Result<Data> {
        Ok(Data {
            id: self.id,
            databoard_id: self.databoard_id,
            name: self.name,
            conf: serde_json::from_slice(&self.conf)?,
            ts: self.ts,
        })
    }
}

pub struct Data {
    pub id: String,
    pub databoard_id: String,
    pub name: String,
    pub conf: DataConf,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    databoard_id CHAR(32) NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, databoard_id: &String, req: CreateUpdateDataReq) -> Result<()> {
    sqlx::query(
        format!(
            "INSERT INTO {} (id, databoard_id, name, conf, ts) VALUES (?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(databoard_id)
    .bind(req.name)
    .bind(serde_json::to_vec(&req.conf)?)
    .bind(common::timestamp_millis() as i64)
    .execute(POOL.get().unwrap())
    .await?;
    Ok(())
}

pub async fn count_by_databoard_id(databoard_id: &String) -> Result<usize> {
    let count: i64 = sqlx::query_scalar(
        format!("SELECT COUNT(*) FROM {} WHERE databoard_id = ?", TABLE_NAME).as_str(),
    )
    .bind(databoard_id)
    .fetch_one(POOL.get().unwrap())
    .await?;
    Ok(count as usize)
}

pub async fn search(
    databoard_id: &String,
    pagination: Pagination,
    query_params: QueryDatasParams,
) -> Result<(usize, Vec<Data>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, db_databoard_datas) = match query_params.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE parent_id = ? AND name LIKE ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(databoard_id)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let databoard_datas = sqlx::query_as::<_, DbData>(
                format!("SELECT * FROM {} WHERE parent_id = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?", TABLE_NAME).as_str(),
            )
            .bind(databoard_id)
            .bind(format!("%{}%", name))
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;
            (count, databoard_datas)
        }
        None => {
            let count: i64 = sqlx::query_scalar(
                format!("SELECT COUNT(*) FROM {} WHERE parent_id = ?", TABLE_NAME).as_str(),
            )
            .bind(databoard_id)
            .fetch_one(POOL.get().unwrap())
            .await?;
            let databoard_datas = sqlx::query_as::<_, DbData>(
                format!(
                    "SELECT * FROM {} WHERE parent_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(databoard_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;
            (count, databoard_datas)
        }
    };

    let databoard_datas = db_databoard_datas
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<_>>>()?;
    Ok((count as usize, databoard_datas))
}

pub async fn read_all_by_databoard_id(databoard_id: &String) -> Result<Vec<Data>> {
    let db_databoard_datas = sqlx::query_as::<_, DbData>(
        format!("SELECT * FROM {} WHERE databoard_id = ?", TABLE_NAME).as_str(),
    )
    .bind(databoard_id)
    .fetch_all(POOL.get().unwrap())
    .await?;

    db_databoard_datas
        .into_iter()
        .map(|x| x.transfer())
        .collect()
}

pub async fn read_one(databoard_data_id: &String) -> Result<Data> {
    let db_databoard_data =
        sqlx::query_as::<_, DbData>(format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(databoard_data_id)
            .fetch_one(POOL.get().unwrap())
            .await?;
    db_databoard_data.transfer()
}

pub async fn update(id: &String, req: CreateUpdateDataReq) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET name = ?, conf = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(serde_json::to_vec(&req.conf)?)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub(crate) async fn delete_many(databoard_id: &String) -> Result<()> {
    sqlx::query(format!("DELETE FROM {} WHERE databoard_id = ?", TABLE_NAME).as_str())
        .bind(databoard_id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn check_exists(id: &String) -> Result<bool> {
    let count: i64 =
        sqlx::query_scalar(format!("SELECT COUNT(*) FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(count == 1)
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    crate::delete_by_id(id, TABLE_NAME).await
}

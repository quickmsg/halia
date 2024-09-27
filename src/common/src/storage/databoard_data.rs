use anyhow::Result;
use sqlx::FromRow;
use types::{
    databoard::{CreateUpdateDataReq, QueryDatasParams},
    Pagination,
};

use super::POOL;

#[derive(FromRow)]
pub struct DataboardData {
    pub id: String,
    pub parent_id: String,
    pub name: String,
    pub des: Option<Vec<u8>>,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS databoard_datas (
    id CHAR(32) PRIMARY KEY,
    parent_id CHAR(32) NOT NULL,
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

pub async fn insert(
    databoard_id: &String,
    databoard_data_id: &String,
    req: CreateUpdateDataReq,
) -> Result<()> {
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = chrono::Utc::now().timestamp();
    sqlx::query("INSERT INTO databoard_datas (id, parent_id, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?)")
        .bind(databoard_data_id)
        .bind(databoard_id)
        .bind(req.base.name)
        .bind(desc)
        .bind(conf)
        .bind(ts)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn search(
    databoard_id: &String,
    pagination: Pagination,
    query_params: QueryDatasParams,
) -> Result<(usize, Vec<DataboardData>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, databoard_datas) = match query_params.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM databoard_datas WHERE parent_id = ? AND name LIKE ?",
            )
            .bind(databoard_id)
            .bind(format!("%{}%", name))
            .fetch_one(POOL.get().unwrap())
            .await?;

            let databoard_datas = sqlx::query_as::<_, DataboardData>(
                "SELECT * FROM databoard_datas WHERE parent_id = ? AND name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
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
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM databoard_datas WHERE parent_id = ?")
                    .bind(databoard_id)
                    .fetch_one(POOL.get().unwrap())
                    .await?;
            let databoard_datas = sqlx::query_as::<_, DataboardData>(
                "SELECT * FROM databoard_datas WHERE parent_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(databoard_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;
            (count, databoard_datas)
        }
    };
    Ok((count as usize, databoard_datas))
}

pub async fn read_all_by_parent_id(databoard_id: &String) -> Result<Vec<DataboardData>> {
    let databoard_datas =
        sqlx::query_as::<_, DataboardData>("SELECT * FROM databoard_datas WHERE parent_id = ?")
            .bind(databoard_id)
            .fetch_all(POOL.get().unwrap())
            .await?;

    Ok(databoard_datas)
}

pub async fn read_one(databoard_data_id: &String) -> Result<DataboardData> {
    let databoard_data =
        sqlx::query_as::<_, DataboardData>("SELECT * FROM databoard_datas WHERE id = ?")
            .bind(databoard_data_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(databoard_data)
}

pub async fn update(id: &String, req: CreateUpdateDataReq) -> Result<()> {
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    let conf = serde_json::to_vec(&req.ext)?;
    sqlx::query("UPDATE databoard_datas SET name = ?, des = ?, conf = ? WHERE id = ?")
        .bind(req.base.name)
        .bind(desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete_one(databoard_data_id: &String) -> Result<()> {
    sqlx::query("DELETE FROM databoard_datas WHERE id = ?")
        .bind(databoard_data_id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub(crate) async fn delete_many(databoard_id: &String) -> Result<()> {
    sqlx::query("DELETE FROM databoard_datas WHERE parent_id = ?")
        .bind(databoard_id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}


pub async fn check_exists(id: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM databoard_datas WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count == 1)
}

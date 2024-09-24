use anyhow::Result;
use sqlx::FromRow;
use types::{databoard::CreateUpdateDataReq, Pagination};

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
    ts BIGINT UNSIGNED NOT NULL                -- 时间戳字段使用 BIGINT 来确保兼容性
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

pub async fn query(databoard_id: &String, pagination: Pagination) -> Result<Vec<DataboardData>> {
    let (limit, offset) = pagination.to_sql();
    let databoard_datas = sqlx::query_as::<_, DataboardData>(
        "SELECT * FROM databoard_datas WHERE parent_id = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
    )
    .bind(databoard_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(POOL.get().unwrap())
    .await?;

    Ok(databoard_datas)
}

pub async fn read_all_by_parent_id(databoard_id: &String) -> Result<Vec<DataboardData>> {
    let databoard_datas =
        sqlx::query_as::<_, DataboardData>("SELECT * FROM databoard_datas WHERE parent_id = ?")
            .bind(databoard_id)
            .fetch_all(POOL.get().unwrap())
            .await?;

    Ok(databoard_datas)
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

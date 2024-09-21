use anyhow::Result;
use sqlx::FromRow;
use types::databoard::CreateUpdateDataReq;

use super::POOL;

#[derive(FromRow)]
pub struct DataboardData {
    pub id: String,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS databoard_datas (
    id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
    name TEXT NOT NULL,
    desc TEXT,
    conf TEXT NOT NULL,
    ts INT NOT NULL
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
    let conf = serde_json::to_string(&req.ext)?;
    let ts = chrono::Utc::now().timestamp();
    sqlx::query("INSERT INTO databoard_datas (id, parent_id, name, desc, conf, ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")
        .bind(databoard_data_id)
        .bind(databoard_id)
        .bind(req.base.name)
        .bind(req.base.desc)
        .bind(conf)
        .bind(ts)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn read_many(databoard_id: &String) -> Result<Vec<DataboardData>> {
    let databoard_datas = sqlx::query_as::<_, DataboardData>(
        "SELECT * FROM databoard_datas WHERE parent_id = ?1 ORDER BY ts DESC",
    )
    .bind(databoard_id)
    .fetch_all(POOL.get().unwrap())
    .await?;

    Ok(databoard_datas)
}

pub async fn update(id: &String, req: CreateUpdateDataReq) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    sqlx::query("UPDATE databoard_datas SET name = ?1, desc = ?2, conf = ?3 WHERE id = ?4")
        .bind(req.base.name)
        .bind(req.base.desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete_one(databoard_data_id: &String) -> Result<()> {
    sqlx::query("DELETE FROM databoard_datas WHERE id = ?1")
        .bind(databoard_data_id.to_string())
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub(crate) async fn delete_many(databoard_id: &String) -> Result<()> {
    sqlx::query("DELETE FROM databoard_datas WHERE parent_id = ?1")
        .bind(databoard_id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

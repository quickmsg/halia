use anyhow::Result;
use common::error::HaliaResult;
use sqlx::prelude::FromRow;
use types::CreateUpdateSourceOrSinkTemplateReq;

use super::POOL;

const TABLE_NAME: &str = "device_source_or_sink_template_references";

#[derive(FromRow)]
pub struct SourceOrSinkTemplateReference {
    pub template_id: String,
    pub reousrce_id: String,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    template_id CHAR(32) NOT NULL,
    resource_id CHAR(32) NOT NULL,
    UNIQUE(template_id, resource_id)
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateUpdateSourceOrSinkTemplateReq) -> HaliaResult<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = common::timestamp_millis();
    sqlx::query(
    format!("INSERT INTO {} (id, status, err, typ, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", TABLE_NAME).as_str(),
    )
    .bind(id)
    .bind(false as i32)
    .bind(false as i32)
    .bind(req.base.name)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn count_by_template_id(template_id: &String) -> Result<usize> {
    let count: i64 = sqlx::query_scalar(
        format!("SELECT COUNT(*) FROM {} WHERE template_id = ?", TABLE_NAME).as_str(),
    )
    .bind(template_id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    Ok(count as usize)
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    crate::delete_by_id(id, TABLE_NAME).await
}

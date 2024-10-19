use common::error::HaliaResult;
use sqlx::prelude::FromRow;

use super::POOL;

const TABLE_NAME: &str = "schema_refs";

#[derive(FromRow)]
pub struct Schema {
    pub schema_id: String,
    pub resource_id: String,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    schema_id CHAR(32) NOT NULL,
    resource_id CHAR(32) NOT NULL,
    UNIQUE(schema_id, resource_id)
);
"#,
        TABLE_NAME,
    )
}

pub async fn insert(schema_id: &String, resource_id: &String) -> HaliaResult<()> {
    sqlx::query(
        format!(
            "INSERT INTO {} (schema_id, resource_id) VALUES (?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(schema_id)
    .bind(resource_id)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn count_by_schema_id(schema_id: &String) -> HaliaResult<i64> {
    let count: i64 = sqlx::query_scalar(
        format!("SELECT COUNT(*) FROM {} WHERE schema_id = ?", TABLE_NAME).as_str(),
    )
    .bind(schema_id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    Ok(count)
}

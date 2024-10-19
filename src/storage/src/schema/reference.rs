use anyhow::Result;
use common::error::HaliaResult;
use sqlx::prelude::FromRow;

use super::POOL;

pub const TABLE_NAME: &str = "schema_refs";

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
    resource_id CHAR(32) NOT NULL
);
"#,
        TABLE_NAME,
    )
}

pub async fn insert(id: &String) -> HaliaResult<()> {
    let ts = common::timestamp_millis();
    sqlx::query(
    format!("INSERT INTO {} (id, status, err, typ, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", TABLE_NAME).as_str()
    )
    .bind(id)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<Schema> {
    let schema = sqlx::query_as::<_, Schema>("SELECT * FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(schema)
}

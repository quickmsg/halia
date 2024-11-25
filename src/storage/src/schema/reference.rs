use anyhow::Result;
use common::error::HaliaResult;
use sqlx::prelude::FromRow;
use types::{
    schema::{ParentType, ResourceType},
    Pagination,
};

use super::POOL;

const TABLE_NAME: &str = "schema_refs";

#[derive(FromRow)]
pub struct DbSchema {
    pub schema_id: String,
    pub parent_type: i32,
    pub parent_id: String,
    pub resource_type: i32,
    pub resource_id: String,
}

impl DbSchema {
    pub fn transfer(self) -> Result<Schema> {
        Ok(Schema {
            schema_id: self.schema_id,
            parent_type: self.parent_type.try_into()?,
            parent_id: self.parent_id,
            resource_type: self.resource_type.try_into()?,
            resource_id: self.resource_id,
        })
    }
}

pub struct Schema {
    pub schema_id: String,
    pub parent_type: ParentType,
    pub parent_id: String,
    pub resource_type: ResourceType,
    pub resource_id: String,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    schema_id CHAR(32) NOT NULL,
    parent_type SMALLINT NOT NULL,
    parent_id CHAR(32) NOT NULL,
    resource_type SMALLINT NOT NULL,
    resource_id CHAR(32) NOT NULL,
    UNIQUE(schema_id, resource_id)
);
"#,
        TABLE_NAME,
    )
}

pub async fn insert_app_source(
    schema_id: &String,
    app_id: &String,
    app_source_id: &String,
) -> HaliaResult<()> {
    insert(
        schema_id,
        ParentType::App,
        app_id,
        ResourceType::Source,
        app_source_id,
    )
    .await
}

pub async fn insert_app_sink(
    schema_id: &String,
    app_id: &String,
    app_sink_id: &String,
) -> HaliaResult<()> {
    insert(
        schema_id,
        ParentType::App,
        app_id,
        ResourceType::Sink,
        app_sink_id,
    )
    .await
}

pub async fn insert_device_source(
    schema_id: &String,
    device_id: &String,
    device_source_id: &String,
) -> HaliaResult<()> {
    insert(
        schema_id,
        ParentType::Device,
        device_id,
        ResourceType::Source,
        device_source_id,
    )
    .await
}

pub async fn insert_device_sink(
    schema_id: &String,
    device_id: &String,
    device_sink_id: &String,
) -> HaliaResult<()> {
    insert(
        schema_id,
        ParentType::Device,
        device_id,
        ResourceType::Sink,
        device_sink_id,
    )
    .await
}

async fn insert(
    schema_id: &String,
    parent_type: ParentType,
    parent_id: &String,
    resource_type: ResourceType,
    resource_id: &String,
) -> HaliaResult<()> {
    sqlx::query(
        format!(
            "INSERT INTO {} (schema_id, parent_type, parent_id, resource_type, resource_id) VALUES (?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(schema_id)
    .bind(Into::<i32>::into(parent_type))
    .bind(parent_id)
    .bind(Into::<i32>::into(resource_type))
    .bind(resource_id)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn count_by_schema_id(schema_id: &String) -> HaliaResult<usize> {
    let count: i64 = sqlx::query_scalar(
        format!("SELECT COUNT(*) FROM {} WHERE schema_id = ?", TABLE_NAME).as_str(),
    )
    .bind(schema_id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    Ok(count as usize)
}

pub async fn query(schema_id: &String, pagination: Pagination) -> HaliaResult<(usize, Vec<Schema>)> {
    let (limit, offset) = pagination.to_sql();
    let count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM {} WHERE schema_id = ? LIMIT ? OFFSET ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(schema_id)
    .bind(limit)
    .bind(offset)
    .fetch_one(POOL.get().unwrap())
    .await?;

    let db_schemas = sqlx::query_as::<_, DbSchema>(
        format!(
            "SELECT * FROM {} WHERE schema_id = ? LIMIT ? OFFSET ?",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(schema_id)
    .fetch_all(POOL.get().unwrap())
    .await?;

    let schemas = db_schemas
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<_>>>()?;

    Ok((count as usize, schemas))
}

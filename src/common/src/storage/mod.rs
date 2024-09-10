use anyhow::Result;
use sqlx::AnyPool;

pub mod app;
pub mod databoard;
pub mod device;
pub mod rule;
pub mod sink;
pub mod source;
pub mod user;

pub async fn create_tables(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS users (
    username TEXT NOT NULL,
    password TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS apps (
    id TEXT PRIMARY KEY,
    status INTEGER NOT NULL,
    conf TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
    conf TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sinks (
    id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
    conf TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS databoards (
    id TEXT PRIMARY KEY,
    conf TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS databoard_datas (
    id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
    conf TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rules (
    id TEXT PRIMARY KEY,
    status INTEGER NOT NULL,
    conf TEXT NOT NULL
);
"#,
    )
    .execute(storage)
    .await?;

    device::init_table(storage).await?;

    Ok(())
}

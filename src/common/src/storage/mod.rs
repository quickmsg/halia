use anyhow::Result;
use sqlx::AnyPool;

pub mod app;
pub mod databoard;
pub mod databoard_data;
pub mod device;
pub mod rule;
pub mod rule_ref;
pub mod source_or_sink;
pub mod user;

pub async fn create_tables(storage: &AnyPool) -> Result<()> {
    device::init_table(storage).await?;
    source_or_sink::init_table(storage).await?;
    rule_ref::init_table(storage).await?;
    databoard::init_table(storage).await?;
    databoard_data::init_table(storage).await?;
    rule::init_table(storage).await?;

    Ok(())
}

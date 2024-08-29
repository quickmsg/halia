use std::sync::Arc;

use anyhow::Result;
use common::persistence;
use sqlx::AnyPool;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    sqlx::any::install_default_drivers();
    let pool = AnyPool::connect("sqlite://db").await?;

    persistence::create_tables(&pool).await?;
    let pool = Arc::new(pool);
    let devices = devices::load_from_persistence(&pool).await.unwrap();
    let apps = apps::load_from_persistence(&pool).await.unwrap();
    let rules = rule::load_from_persistence(&pool, &devices, &apps)
        .await
        .unwrap();

    info!("server starting...");
    api::start(pool, devices, apps, rules).await;

    Ok(())
}

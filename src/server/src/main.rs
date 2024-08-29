use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use common::persistence;
use sqlx::{any::AnyConnectOptions, AnyPool, ConnectOptions};
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
    let opt = AnyConnectOptions::from_str("sqlite://db")
        .unwrap()
        .disable_statement_logging();
    let pool = AnyPool::connect_with(opt).await?;

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

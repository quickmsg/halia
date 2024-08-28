use std::sync::Arc;

use anyhow::Result;
use common::persistence::{self, Persistence};
use tokio::sync::Mutex;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let local_persistence = persistence::local::Local::new("./db").unwrap();
    local_persistence.init().unwrap();
    let persistence = Arc::new(Mutex::new(local_persistence));

    let devices = devices::load_from_persistence(&persistence).await.unwrap();
    let apps = apps::load_from_persistence(&persistence).await.unwrap();
    let rules = rule::load_from_persistence(&persistence, &devices, &apps)
        .await
        .unwrap();

    info!("server starting...");
    api::start(persistence, devices, apps, rules).await;

    Ok(())
}

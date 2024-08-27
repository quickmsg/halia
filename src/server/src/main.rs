use anyhow::Result;
use apps::GLOBAL_APP_MANAGER;
use common::persistence::{self, Persistence};
use devices::GLOBAL_DEVICE_MANAGER;
use rule::GLOBAL_RULE_MANAGER;
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

    // persistence::init_dir().await.unwrap();
    // GLOBAL_DEVICE_MANAGER.recover().await.unwrap();
    // GLOBAL_APP_MANAGER.recover().await.unwrap();
    // GLOBAL_RULE_MANAGER.recover().await.unwrap();

    info!("server starting...");
    api::start(local_persistence).await;

    Ok(())
}

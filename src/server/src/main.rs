use anyhow::Result;
use apps::GLOBAL_APP_MANAGER;
use devices::GLOBAL_DEVICE_MANAGER;
use rule::GLOBAL_RULE_MANAGER;
use tracing::{debug, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    GLOBAL_DEVICE_MANAGER.recover().await.unwrap();
    GLOBAL_APP_MANAGER.recover().await.unwrap();
    GLOBAL_RULE_MANAGER.recover().await.unwrap();

    info!("server starting...");
    api::start().await;

    Ok(())
}

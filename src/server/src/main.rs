use anyhow::Result;
use api::start;
use device::GLOBAL_DEVICE_MANAGER;
use rule::GLOBAL_RULE_MANAGER;
use sink::GLOBAL_SINK_MANAGER;
use source::GLOBAL_SOURCE_MANAGER;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    GLOBAL_DEVICE_MANAGER.recover().await.unwrap();
    GLOBAL_SOURCE_MANAGER.recover().await.unwrap();
    GLOBAL_SINK_MANAGER.recover().await.unwrap();
    GLOBAL_RULE_MANAGER.recover().await.unwrap();

    start().await;
    info!("server start");
    Ok(())
}

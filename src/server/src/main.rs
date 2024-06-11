use anyhow::Result;
use api::start;
use device::GLOBAL_DEVICE_MANAGER;
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

    start().await;
    info!("server start");
    Ok(())
}

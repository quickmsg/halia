use anyhow::Result;
use api::start;
use device::GLOBAL_DEVICE_MANAGER;
use tracing::{debug, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    match GLOBAL_DEVICE_MANAGER.recover().await {
        Ok(_) => {},
        Err(e) => panic!("{:?}", e),
    }

    // GLOBAL_DEVICE_MANAGER.recover().await.unwrap();
    debug!("recover done");

    start().await;
    info!("server start");
    Ok(())
}

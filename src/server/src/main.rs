use api::start;
use device::GLOBAL_DEVICE_MANAGER;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    info!("server start");
    let _ = GLOBAL_DEVICE_MANAGER.recover().await;
    start().await;
}

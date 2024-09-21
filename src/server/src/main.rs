use std::env;

use anyhow::Result;
use common::{config, storage, sys};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let config_path;
    if args.len() < 3 {
        config_path = "./config.toml";
    } else {
        config_path = &args[2];
    }

    let config = config::init(config_path);

    let level = match config.log_level {
        config::LogLevel::Error => Level::ERROR,
        config::LogLevel::Warn => Level::WARN,
        config::LogLevel::Info => Level::INFO,
        config::LogLevel::Debug => Level::DEBUG,
        config::LogLevel::Trace => Level::TRACE,
    };
    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        // TODO 发布环境去除
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    sys::init();

    storage::init(&config.storage).await?;

    devices::load_from_storage().await.unwrap();
    apps::load_from_storage().await.unwrap();
    databoard::load_from_storage().await.unwrap();
    rule::load_from_storage().await.unwrap();

    info!("server starting...");
    api::start(config.port).await;

    Ok(())
}

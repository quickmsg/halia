use std::{
    env::{self},
    fs::File,
    path::Path,
    str::FromStr,
    sync::Arc,
};

use anyhow::Result;
use common::{config, storage, sys};
use sqlx::{any::AnyConnectOptions, AnyPool, ConnectOptions};
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

    // todo 按需引入driver
    sqlx::any::install_default_drivers();
    let opt = match config.storage {
        config::Storage::Sqlite(sqlite) => {
            let path = Path::new(&sqlite.path);
            if !path.exists() {
                File::create(&sqlite.path)?;
            }
            AnyConnectOptions::from_str("sqlite://db")
                .unwrap()
                .disable_statement_logging()
        }
        config::Storage::Mysql(_) => {
            AnyConnectOptions::from_str("mysql://root:my-secret-pw@192.168.124.39:3306/halia")
                .unwrap()
                .disable_statement_logging()
        }
        config::Storage::Postgresql(_) => todo!(),
    };

    let storage = AnyPool::connect_with(opt).await?;
    storage::create_tables(&storage).await?;

    let storage = Arc::new(storage);
    let devices = devices::load_from_storage(&storage).await.unwrap();
    let apps = apps::load_from_persistence(&storage).await.unwrap();
    let databoards = databoard::load_from_storage(&storage).await.unwrap();
    let rules = rule::load_from_storage(&storage, &devices, &apps, &databoards)
        .await
        .unwrap();

    info!("server starting...");
    api::start(config.port, storage, devices, apps, databoards, rules).await;

    Ok(())
}

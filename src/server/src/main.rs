use std::{env, fs, path::Path};

use anyhow::Result;
use common::{config, sys};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{info, Level};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

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
    let filter = EnvFilter::new("trace,rskafka=off");
    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_env_filter(filter)
        // TODO 发布环境去除
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    sys::init();

    let path = Path::new("logs");
    if !path.exists() {
        fs::create_dir(path)?;
    }

    let sched = JobScheduler::new().await?;
    let event_retain_days = config.event_retain_days;
    sched
        .add(Job::new_async("0 3 * * * *", {
            let event_retain_days = event_retain_days.clone();
            move |_uuid, _l| {
                Box::pin(async move {
                    storage::event::delete_expired(event_retain_days)
                        .await
                        .unwrap();
                })
            }
        })?)
        .await?;
    sched.start().await?;

    storage::init(&config.storage).await?;

    devices::load_from_storage().await.unwrap();
    apps::load_from_storage().await.unwrap();
    databoard::load_from_storage().await.unwrap();
    rule::load_from_storage().await.unwrap();

    info!("server starting on {}...", config.port);
    api::start(config.port).await;

    Ok(())
}
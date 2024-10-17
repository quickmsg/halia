use std::{fs::File, path::Path, str::FromStr, sync::LazyLock};

use anyhow::Result;
use sqlx::{any::AnyConnectOptions, AnyPool, ConnectOptions as _};
use tokio::sync::OnceCell;

use common::config::StorageConfig;

static POOL: LazyLock<OnceCell<AnyPool>> = LazyLock::new(OnceCell::new);

pub mod app;
pub mod databoard;
pub mod databoard_data;
pub mod device;
pub mod event;
pub mod rule;
pub mod rule_ref;
pub mod schema;
pub mod source_or_sink;
pub mod user;

pub async fn init(config: &StorageConfig) -> Result<()> {
    sqlx::any::install_default_drivers();
    let opt = match config {
        StorageConfig::Sqlite(sqlite) => {
            let path = Path::new(&sqlite.path);
            if !path.exists() {
                File::create(&sqlite.path)?;
            }
            AnyConnectOptions::from_str("sqlite://db")
                .unwrap()
                .disable_statement_logging()
        }
        StorageConfig::Mysql(mysql) => AnyConnectOptions::from_str(
            format!(
                "mysql://{}:{}@{}:{}/{}",
                mysql.username, mysql.password, mysql.host, mysql.port, mysql.db_name
            )
            .as_str(),
        )
        .unwrap()
        .disable_statement_logging(),
        StorageConfig::Postgresql(_) => todo!(),
    };

    let pool = AnyPool::connect_with(opt).await?;
    POOL.set(pool).unwrap();

    device::init_table().await.unwrap();
    app::init_table().await?;
    source_or_sink::init_table().await?;
    databoard::init_table().await?;
    databoard_data::init_table().await?;
    rule_ref::init_table().await?;
    rule::init_table().await?;
    event::init_table().await?;
    user::init_table().await?;

    Ok(())
}

pub async fn insert_name_exists(name: &String, table_name: &str) -> Result<bool> {
    let count: i64 =
        sqlx::query_scalar(format!("SELECT COUNT(*) FROM {} WHERE name = ?", table_name).as_str())
            .bind(name)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(count > 0)
}

pub async fn update_name_exists(id: &String, name: &String, table_name: &str) -> Result<bool> {
    let count: i64 = sqlx::query_scalar(
        format!(
            "SELECT COUNT(*) FROM {} WHERE name = ? AND id != ?",
            table_name
        )
        .as_str(),
    )
    .bind(name)
    .bind(id)
    .fetch_one(POOL.get().unwrap())
    .await?;

    Ok(count > 0)
}
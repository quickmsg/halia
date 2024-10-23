use std::{fs::File, path::Path, str::FromStr, sync::LazyLock};

use anyhow::Result;
use sqlx::{any::AnyConnectOptions, AnyPool, ConnectOptions as _};
use tokio::sync::OnceCell;

use common::{config::StorageConfig, error::HaliaResult};

static POOL: LazyLock<OnceCell<AnyPool>> = LazyLock::new(OnceCell::new);

pub mod app;
pub mod databoard;
pub mod device;
pub mod event;
pub mod rule;
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

    sqlx::query(&device::source_sink_template::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();

    sqlx::query(&device::template::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&device::template_source_sink::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&device::device::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&device::source_sink::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();

    sqlx::query(&app::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&databoard::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&databoard::data::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&source_or_sink::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&rule::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&rule::reference::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&schema::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&schema::reference::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&user::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();
    sqlx::query(&event::create_table())
        .execute(POOL.get().unwrap())
        .await
        .unwrap();

    Ok(())
}

async fn delete_by_id(id: &String, table_name: &str) -> HaliaResult<()> {
    sqlx::query(format!("DELETE FROM {} WHERE id = ?", table_name).as_str())
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub enum SourceSinkType {
    Source,
    Sink,
}

impl From<i32> for SourceSinkType {
    fn from(i: i32) -> Self {
        match i {
            1 => SourceSinkType::Source,
            2 => SourceSinkType::Sink,
            _ => panic!("invalid type"),
        }
    }
}

impl Into<i32> for SourceSinkType {
    fn into(self) -> i32 {
        match self {
            SourceSinkType::Source => 1,
            SourceSinkType::Sink => 2,
        }
    }
}

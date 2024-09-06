use std::fs;

use serde::Deserialize;
use tracing::info;

pub fn init(config_path: &str) -> Config {
    match fs::read_to_string(config_path) {
        Ok(contents) => {
            let mut config_raw: ConfigRaw = toml::from_str(&contents).unwrap();
            let port = match config_raw.port.take() {
                Some(port) => port,
                None => 13000,
            };

            let log_level = match config_raw.log_level.take() {
                Some(log_level) => log_level,
                None => LogLevel::Info,
            };

            let storage = match config_raw.storage.take() {
                Some(storage) => storage,
                None => Storage::Sqlite(Sqlite {
                    path: "./db".to_string(),
                }),
            };

            let event_retain_days = match config_raw.event_retain_days.take() {
                Some(event_retain_days) => event_retain_days,
                None => 7,
            };

            Config {
                port,
                log_level,
                storage,
                event_retain_days,
            }
        }
        Err(_) => {
            info!("未找到配置文件，使用默认配置！");
            Config::default()
        }
    }
}

#[derive(Deserialize)]
struct ConfigRaw {
    // halia服务的端口，默认为13000
    pub port: Option<u16>,
    // 日志等级
    pub log_level: Option<LogLevel>,
    // 存储类型：目前支持sqlite，mysql，postgresql。默认为sqlite
    pub storage: Option<Storage>,
    // 事件保留时间，默认为7天
    pub event_retain_days: Option<u8>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

pub struct Config {
    pub port: u16,
    pub log_level: LogLevel,
    pub storage: Storage,
    pub event_retain_days: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 13000,
            log_level: LogLevel::Info,
            storage: Storage::Sqlite(Sqlite {
                path: "./db".to_string(),
            }),
            event_retain_days: 7,
        }
    }
}

#[derive(Deserialize)]
pub enum Storage {
    Sqlite(Sqlite),
    Mysql(Mysql),
    Postgresql(Postgresql),
}

#[derive(Deserialize)]
pub struct Sqlite {
    pub path: String,
}

#[derive(Deserialize)]
pub struct Mysql {}

#[derive(Deserialize)]
pub struct Postgresql {}

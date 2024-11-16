use anyhow::bail;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub mod apps;
pub mod databoard;
pub mod devices;
pub mod events;
pub mod mqtt_server;
pub mod rules;
pub mod schema;
pub mod user;

#[derive(Serialize)]
pub struct Dashboard {
    pub machine_info: MachineInfo,
    pub device_summary: devices::Summary,
    pub app_summary: apps::Summary,
    pub databoard_summary: databoard::Summary,
    pub rule_summary: rules::Summary,
}

#[derive(Serialize)]
pub struct MachineInfo {
    pub start_time: u64,
    pub total_memory: u64,
    pub used_memory: u64,
    pub halia_memory: u64,
    pub global_cpu_usage: f32,
    pub disks: Vec<(String, u64, u64)>,
}

#[derive(Debug, Deserialize)]
pub struct Pagination {
    #[serde(rename = "p")]
    pub page: usize,
    #[serde(rename = "s")]
    pub size: usize,
}

impl Pagination {
    pub fn check(&self, index: usize) -> bool {
        index >= (self.page - 1) * self.size && index < self.page * self.size
    }

    pub fn to_sql(&self) -> (i64, i64) {
        ((self.size as i64), ((self.page - 1) * self.size) as i64)
    }
}

#[derive(Deserialize)]
pub struct Value {
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SslConf {
    pub verify: bool,
    pub alpn: Option<PlainOrBase64Value>,
    pub ca_cert: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug)]
pub struct MessageRetain {
    #[serde(rename = "type")]
    pub typ: MessageRetainType,
    pub count: Option<usize>,
    pub time: Option<u64>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum MessageRetainType {
    All,
    None,
    LatestCount,
    LatestTime,
}

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug)]
pub struct PlainOrBase64Value {
    #[serde(rename = "type")]
    pub typ: PlainOrBase64ValueType,
    pub value: String,
}

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum PlainOrBase64ValueType {
    Plain,
    Base64,
}

impl Into<Vec<u8>> for PlainOrBase64Value {
    fn into(self) -> Vec<u8> {
        match self.typ {
            PlainOrBase64ValueType::Plain => self.value.into_bytes(),
            PlainOrBase64ValueType::Base64 => BASE64_STANDARD.decode(&self.value).unwrap(),
        }
    }
}

impl Into<Bytes> for PlainOrBase64Value {
    fn into(self) -> Bytes {
        match self.typ {
            PlainOrBase64ValueType::Plain => Bytes::from(self.value.into_bytes()),
            PlainOrBase64ValueType::Base64 => {
                Bytes::from(BASE64_STANDARD.decode(&self.value).unwrap())
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Running,
    Stopped,
    Error,
}

impl Default for Status {
    fn default() -> Self {
        Status::Stopped
    }
}

impl Into<i32> for Status {
    fn into(self) -> i32 {
        match self {
            Status::Running => 1,
            Status::Stopped => 2,
            Status::Error => 3,
        }
    }
}

impl TryFrom<i32> for Status {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, anyhow::Error> {
        match value {
            1 => Ok(Status::Running),
            2 => Ok(Status::Stopped),
            3 => Ok(Status::Error),
            _ => bail!("invalid value: {}", value),
        }
    }
}

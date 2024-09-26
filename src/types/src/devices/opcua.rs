use serde::{Deserialize, Serialize};

use crate::BaseConf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateOpcuaReq {
    pub base: BaseConf,
    pub ext: OpcuaConf,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct OpcuaConf {
    pub host: String,
    pub port: u16,
    // 秒数
    pub reconnect: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct CreaetUpdateSourceReq {
    pub base: BaseConf,
    pub ext: SourceConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct SourceConf {
    #[serde(rename = "type")]
    pub typ: SourceType,
    #[serde(flatten)]
    pub group: Option<GroupConf>,
    pub subscription: Option<Subscriptionconf>,
    pub monitored_item: Option<MonitoredItemconf>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Group,
    Subscription,
    MonitoredItem,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct GroupConf {
    // 毫秒
    pub interval: u64,
    pub timestamps_to_return: TimestampsToReturn,
    // pub max_age: f64,
    // serde_json bug,无法在flatten模式下解析f64
    pub max_age: f64,
    pub variables: Vec<VariableConf>,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampsToReturn {
    Source,
    Server,
    Both,
    Neither,
    Invalid,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct VariableConf {
    pub field: String,
    pub namespace: u16,
    pub identifier_type: IdentifierType,
    pub identifier: serde_json::Value,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierType {
    Numeric,
    String,
    Guid,
    ByteString,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Subscriptionconf {
    // 秒
    pub publishing_interval: u64,

    pub lifetime_count: u32,
    pub max_keep_alive_count: u32,
    pub max_notifications_per_publish: u32,
    pub priority: u8,
    pub publishing_enalbed: bool,

    pub monitored_items: Vec<VariableConf>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct MonitoredItemconf {
    #[serde(flatten)]
    pub variable: VariableConf,
    pub attribute_id: u32,
    pub index_range: String,
    pub data_encoding: String,
    pub monitoring_mode: MonitoringMode,

    pub client_handle: u32,
    pub sampling_interval: f64,
    // pub filter:
    pub queue_size: u32,
    pub discard_oleds: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MonitoringMode {
    Disabled,
    Sampling,
    Reporting,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct SinkConf {}

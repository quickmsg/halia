use serde::{Deserialize, Serialize};

use crate::devices::device::opcua::{NodeId, SourceType, VariableConf};

#[derive(Deserialize, Serialize, Debug)]
pub struct SourceCustomizeConf {}

#[derive(Deserialize, Serialize, Debug)]
pub struct SourceTemplateConf {
    #[serde(rename = "type")]
    pub typ: SourceType,
    pub group: Option<GroupConf>,
    pub subscription: Option<Subscriptionconf>,
    pub monitored_item: Option<MonitoredItemconf>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct GroupConf {
    // 毫秒
    pub interval: u64,
    // pub max_age: f64,
    // serde_json bug,无法在flatten模式下解析f64
    pub max_age: f64,
    pub variables: Vec<VariableConf>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Subscriptionconf {
    // 秒
    pub publishing_interval: u64,

    pub lifetime_count: u32,
    pub max_keep_alive_count: u32,
    pub max_notifications_per_publish: u32,
    pub priority: u8,
    pub publishing_enabled: bool,

    pub monitored_items: Vec<MonitoredItemconf>,
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
    pub discard_oldest: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MonitoringMode {
    Disabled,
    Sampling,
    Reporting,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SinkCustomizeConf {
    pub field: String,
    pub node_id: NodeId,
}
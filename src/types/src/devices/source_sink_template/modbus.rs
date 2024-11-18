use serde::{Deserialize, Serialize};

use crate::{
    devices::device::modbus::{Area, DataType},
    MessageRetain,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct SourceCustomizeConf {
    pub slave: u8,
    pub metadata: Option<Vec<(String, serde_json::Value)>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SourceTemplateConf {
    // 字段名称
    pub field: String,
    #[serde(flatten)]
    pub data_type: DataType,
    pub area: Area,
    pub address: u16,
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SinkCustomizeConf {
    pub slave: u8,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SinkTemplateConf {
    pub area: Area,
    pub address: u16,
    #[serde(flatten)]
    pub data_type: DataType,
    pub value: serde_json::Value,
    pub message_retain: MessageRetain,
}

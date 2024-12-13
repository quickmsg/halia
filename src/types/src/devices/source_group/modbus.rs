use serde::{Deserialize, Serialize};

use crate::devices::device::modbus::{Area, DataType};

#[derive(Deserialize, Serialize, Debug)]
pub struct DeviceSourceGroupConf {
    pub slave: u8,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CustomizeConf {
    pub slave: u8,
    pub metadatas: Option<Vec<(String, serde_json::Value)>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TemplateConf {
    // 字段名称
    pub field: String,
    #[serde(flatten)]
    pub data_type: DataType,
    pub area: Area,
    pub address: u16,
    pub interval: u64,
}

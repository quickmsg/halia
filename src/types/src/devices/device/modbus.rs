use serde::{Deserialize, Serialize};

use crate::{
    devices::modbus::{Area, DataBits, DataType, Encode, LinkType, Mode, Parity, StopBits},
    MessageRetain,
};

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Conf {
    pub link_type: LinkType,

    // ms
    pub interval: u64,
    // s
    pub reconnect: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ethernet: Option<Ethernet>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial: Option<Serial>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Ethernet {
    pub mode: Mode,
    pub encode: Encode,
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Serial {
    pub path: String,
    pub stop_bits: StopBits,
    pub baud_rate: u32,
    pub data_bits: DataBits,
    pub parity: Parity,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SourceConf {
    pub slave: u8,
    // 字段名称
    pub field: String,
    #[serde(flatten)]
    pub data_type: DataType,
    pub area: Area,
    pub address: u16,
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SinkConf {
    pub slave: u8,
    #[serde(flatten)]
    pub data_type: DataType,
    pub area: Area,
    pub address: u16,
    pub value: serde_json::Value,
    pub message_retain: MessageRetain,
}

use serde::{Deserialize, Serialize};

use crate::devices::modbus::{DataBits, Encode, LinkType, Mode, Parity, StopBits};

#[derive(Deserialize, Serialize, Debug)]
pub struct CustomizeConf {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ethernet: Option<EthernetCustomizeConf>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial: Option<SerialCustomizeConf>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct EthernetCustomizeConf {
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SerialCustomizeConf {
    pub path: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct TemplateConf {
    pub link_type: LinkType,
    pub interval: u64,  // ms
    pub reconnect: u64, // s

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ethernet: Option<EthernetTemplateConf>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial: Option<SerialTemplateConf>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct EthernetTemplateConf {
    pub mode: Mode,
    pub encode: Encode,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SerialTemplateConf {
    pub stop_bits: StopBits,
    pub baud_rate: u32,
    pub data_bits: DataBits,
    pub parity: Parity,
}

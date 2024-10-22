use serde::{Deserialize, Serialize};

use crate::devices::modbus::{DataBits, Encode, Mode, Parity, StopBits};

#[derive(Deserialize, Serialize)]
pub struct Conf {
    pub link_type: LinkType,

    // ms
    pub interval: u64,
    // s
    pub reconnect: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub ethernet: Option<Ethernet>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub serial: Option<Serial>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LinkType {
    Ethernet,
    Serial,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Ethernet {
    pub mode: Mode,
    pub encode: Encode,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Serial {
    pub stop_bits: StopBits,
    pub baud_rate: u32,
    pub data_bits: DataBits,
    pub parity: Parity,
}
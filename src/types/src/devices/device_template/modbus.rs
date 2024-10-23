use serde::{Deserialize, Serialize};

use crate::devices::modbus::{DataBits, Encode, LinkType, Mode, Parity, StopBits};

#[derive(Deserialize, Serialize, Debug)]
pub struct Conf {
    pub link_type: LinkType,
    pub interval: u64,  // ms
    pub reconnect: u64, // s

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ethernet: Option<Ethernet>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial: Option<Serial>,
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
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use super::{datatype::DataType, SinkValue};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateModbusReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
    pub link_type: LinkType,

    pub interval: u64,

    // 以太网配置
    pub mode: Mode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encode: Option<Encode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    // 串口配置
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_bits: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baud_rate: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_bits: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parity: Option<u8>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum LinkType {
    Ethernet,
    Serial,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Client,
    Server,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Encode {
    Tcp,
    RtuOverTcp,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateGroupReq {
    pub name: String,
    pub interval: u64,
    pub desc: Option<String>,
}

#[derive(Serialize)]
pub struct SearchGroupsResp {
    pub total: usize,
    pub data: Vec<SearchGroupsItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupsItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateGroupReq,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateGroupPointReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
}

#[derive(Deserialize_repr, Serialize_repr, Debug, Clone)]
#[repr(u8)]
pub enum Area {
    InputDiscrete = 1,
    Coils = 2, // 可读写
    InputRegisters = 3,
    HoldingRegisters = 4, // 可读写
}

#[derive(Serialize)]
pub struct SearchGroupPointsResp {
    pub total: usize,
    pub data: Vec<SearchGroupPointsItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupPointsItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateGroupPointReq,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateSinkReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub r#type: DataType,
    pub slave: SinkValue,
    pub area: SinkValue,
    pub address: SinkValue,
    pub value: SinkValue,
}

#[derive(Serialize)]
pub struct SearchSinksResp {
    pub total: usize,
    pub data: Vec<SearchSinksItemResp>,
}

#[derive(Serialize)]
pub struct SearchSinksItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
}

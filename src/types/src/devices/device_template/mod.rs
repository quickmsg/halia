use serde::{Deserialize, Serialize};

use super::DeviceType;

pub mod source_sink;

pub mod coap;
pub mod modbus;
pub mod opcua;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateReq {
    pub name: String,
    pub device_type: DeviceType,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct UpdateReq {
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct QueryParams {
    pub name: Option<String>,
    pub device_type: Option<DeviceType>,
}

#[derive(Serialize)]
pub struct SearchResp {
    pub total: usize,
    pub data: Vec<SearchItemResp>,
}

#[derive(Serialize)]
pub struct SearchItemResp {
    pub id: String,
    pub req: CreateReq,
}

#[derive(Serialize)]
pub struct ReadResp {
    pub id: String,
    pub req: CreateReq,
    pub devices: Vec<ReadRespDeviceItem>,
}

// TODO
#[derive(Serialize)]
pub struct ReadRespDeviceItem {
    pub id: String,
    pub name: String,
    pub address: String,
}
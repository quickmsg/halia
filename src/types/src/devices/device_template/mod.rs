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
pub struct ListResp {
    pub count: usize,
    pub list: Vec<ListItem>,
}

#[derive(Serialize)]
pub struct ListItem {
    pub id: String,
    pub name: String,
    pub device_type: DeviceType,
    pub reference_cnt: usize,
    pub source_cnt: usize,
    pub sink_cnt: usize,
}

#[derive(Serialize)]
pub struct ReadResp {
    pub id: String,
    pub name: String,
    pub device_type: DeviceType,
    pub reference_cnt: usize,
    pub can_delete: bool,
    pub conf: serde_json::Value,
}
use serde::{Deserialize, Serialize};

use super::DeviceType;

pub mod modbus;
pub mod opcua;

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateReq {
    pub name: String,
    pub device_type: DeviceType,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Clone)]
pub struct UpdateReq {
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Debug, Deserialize)]
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
}

#[derive(Serialize)]
pub struct ReadResp {
    pub id: String,
    pub name: String,
    pub device_type: DeviceType,
    pub reference_cnt: usize,
    pub conf: serde_json::Value,
}

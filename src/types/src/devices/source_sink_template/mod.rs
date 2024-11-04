use serde::{Deserialize, Serialize};

use super::DeviceType;

pub mod modbus;

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
pub struct SearchResp {
    pub total: usize,
    pub data: Vec<SearchItemResp>,
}

#[derive(Serialize)]
pub struct SearchItemResp {
    pub id: String,
    pub req: CreateReq,
}

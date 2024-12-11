use serde::{Deserialize, Serialize};

use super::DeviceType;

pub mod modbus;

#[derive(Serialize, Deserialize)]
pub struct CreateReq {
    pub name: String,
    pub device_type: DeviceType,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateReq {
    pub name: String,
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub device_type: Option<DeviceType>,
}

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

#[derive(Serialize, Deserialize)]
pub struct CreateUpdateSourceReq {
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Deserialize)]
pub struct SourceQueryParams {
    pub name: Option<String>,
}

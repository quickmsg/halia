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
    pub source_cnt: usize,
    pub reference_cnt: usize,
}

#[derive(Serialize)]
pub struct ReadResp {
    pub id: String,
    pub name: String,
    pub device_type: DeviceType,
    pub source_cnt: usize,
    pub reference_cnt: usize,
}

#[derive(Serialize, Deserialize)]
pub struct CreateUpdateSourceReq {
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Debug)]
pub struct SourceQueryParams {
    pub name: Option<String>,
}

#[derive(Serialize)]
pub struct ListSourcesResp {
    pub count: usize,
    pub list: Vec<ListSourcesItem>,
}

#[derive(Serialize)]
pub struct ListSourcesItem {
    pub id: String,
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Serialize)]
pub struct ReadSourceResp {
    pub id: String,
    pub name: String,
    pub conf: serde_json::Value,
}

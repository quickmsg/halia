use serde::{Deserialize, Serialize};

use crate::BaseConf;

use super::DeviceType;

pub mod modbus;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateUpdateReq {
    pub typ: DeviceType,
    pub base: BaseConf,
    pub ext: serde_json::Value,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct QueryParams {
    pub name: Option<String>,
    pub typ: Option<DeviceType>,
}

#[derive(Serialize)]
pub struct SearchResp {
    pub total: usize,
    pub data: Vec<SearchItemResp>,
}

#[derive(Serialize)]
pub struct SearchItemResp {
    pub id: String,
    pub typ: DeviceType,
    pub base: BaseConf,
    pub ext: serde_json::Value,
}

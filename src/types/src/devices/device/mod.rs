use serde::{Deserialize, Serialize};

use crate::Status;

use super::{ConfType, DeviceType};

pub mod coap;
pub mod modbus;
pub mod opcua;
pub mod s7;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateReq {
    pub name: String,
    pub device_type: DeviceType,
    pub conf_type: ConfType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct UpdateReq {
    pub name: String,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub conf: serde_json::Value,
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub device_type: Option<DeviceType>,
    pub status: Option<Status>,
    pub template_id: Option<String>,
}

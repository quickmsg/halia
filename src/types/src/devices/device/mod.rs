use serde::{Deserialize, Serialize};
use source_sink::CreateUpdateReq;

use crate::Status;

use super::{ConfType, DeviceType};

pub mod coap;
pub mod modbus;
pub mod opcua;
pub mod s7;
pub mod source_sink;

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
}

#[derive(Serialize)]
pub struct ListDevicesResp {
    pub count: usize,
    pub list: Vec<ListDevicesItem>,
}

#[derive(Serialize)]
pub struct ListDevicesItem {
    pub id: String,
    pub device_type: DeviceType,
    pub name: String,
    pub status: Status,
    pub err: Option<String>,
    pub rule_reference_running_cnt: usize,
    pub rule_reference_total_cnt: usize,
    pub source_cnt: usize,
    pub sink_cnt: usize,
    pub can_stop: bool,
    pub can_delete: bool,
}

#[derive(Deserialize)]
pub struct QueryRuleInfoParams {
    pub device_id: String,
    pub source_id: Option<String>,
    pub sink_id: Option<String>,
}

// #[derive(Serialize)]
// pub struct SearchRuleInfo {
//     pub device: SearchItemResp,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub source: Option<SearchRuleSourceSinkResp>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub sink: Option<SearchRuleSourceSinkResp>,
// }

#[derive(Serialize)]
pub struct SearchRuleSourceSinkResp {
    pub id: String,
    pub req: CreateUpdateReq,
}

use serde::{Deserialize, Serialize};
use source_sink::CreateUpdateReq;

use super::{ConfType, DeviceType};

pub mod modbus;
pub mod source_sink;
pub mod opcua;
pub mod coap;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateReq {
    pub name: String,
    pub device_type: DeviceType,
    pub conf_type: ConfType,
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

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub device_type: Option<DeviceType>,
    pub on: Option<bool>,
    pub err: Option<bool>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running_info: Option<RunningInfo>,
    pub source_cnt: usize,
    pub sink_cnt: usize,
}

#[derive(Serialize)]
pub struct RunningInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
    pub rtt: u16,
}

#[derive(Deserialize)]
pub struct QueryRuleInfoParams {
    pub device_id: String,
    pub source_id: Option<String>,
    pub sink_id: Option<String>,
}

#[derive(Serialize)]
pub struct SearchRuleInfo {
    pub device: SearchItemResp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SearchRuleSourceSinkResp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<SearchRuleSourceSinkResp>,
}


#[derive(Serialize)]
pub struct SearchRuleSourceSinkResp {
    pub id: String,
    pub req: CreateUpdateReq,
}
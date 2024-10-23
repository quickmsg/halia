use serde::{Deserialize, Serialize};

use crate::BaseConf;

use super::{ConfType, DeviceType};

pub mod source_sink;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateUpdateReq {
    pub device_type: DeviceType,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub base: BaseConf,
    pub conf: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub typ: Option<DeviceType>,
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
    pub req: CreateUpdateReq,
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
    pub source: Option<source_sink::CreateUpdateReq>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<source_sink::CreateUpdateReq>,
}

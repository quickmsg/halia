use serde::{Deserialize, Serialize};

use crate::{RuleRefCnt, Status};

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
    pub on: usize,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub status: Option<Status>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateUpdateDataboardReq {
    pub name: String,
}

#[derive(Serialize)]
pub struct ListDataboardsResp {
    pub count: usize,
    pub list: Vec<ListDataboardsItem>,
}

#[derive(Serialize)]
pub struct ListDataboardsItem {
    pub id: String,
    pub name: String,
    pub status: Status,
    pub data_count: usize,
    #[serde(flatten)]
    pub rule_ref_cnt: RuleRefCnt,
    pub can_stop: bool,
    pub can_delete: bool,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct CreateUpdateDataReq {
    pub name: String,
    pub conf: DataConf,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct DataConf {
    pub field: String,
}

#[derive(Deserialize)]
pub struct QueryDatasParams {
    pub name: Option<String>,
}

#[derive(Serialize)]
pub struct ListDatasResp {
    pub count: usize,
    pub list: Vec<ListDatasItemResp>,
}

#[derive(Serialize)]
pub struct ListDatasItemResp {
    pub id: String,
    pub name: String,
    pub conf: DataConf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ts: Option<u64>,
    pub rule_ref_cnt: RuleRefCnt,
    pub can_delete: bool,
}

#[derive(Serialize)]
pub struct SearchDatasInfoResp {
    pub id: String,
    pub conf: CreateUpdateDataReq,
    pub value: Option<serde_json::Value>,
    pub ts: Option<u64>,
}

pub struct SearchDatasRuntimeResp {
    pub value: serde_json::Value,
    pub ts: u64,
}

#[derive(Deserialize)]
pub struct QueryRuleInfo {
    pub databoard_id: String,
    pub data_id: String,
}

#[derive(Serialize)]
pub struct RuleInfoResp {
    pub databoard: RuleInfoDataboard,
    pub data: RuleInfoData,
}

#[derive(Serialize)]
pub struct RuleInfoDataboard {
    pub id: String,
    pub name: String,
    pub status: Status,
}

#[derive(Serialize)]
pub struct RuleInfoData {
    pub id: String,
    pub name: String,
}

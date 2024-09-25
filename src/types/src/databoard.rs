use serde::{Deserialize, Serialize};

use crate::{BaseConf, RuleRef};

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
    pub on: usize,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub on: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateUpdateDataboardReq {
    pub base: BaseConf,
    pub ext: DataboardConf,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct DataboardConf {}

#[derive(Serialize)]
pub struct SearchDataboardsResp {
    pub total: usize,
    pub data: Vec<SearchDataboardsItemResp>,
}

#[derive(Serialize)]
pub struct SearchDataboardsItemResp {
    pub id: String,
    pub on: bool,
    pub conf: CreateUpdateDataboardReq,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct CreateUpdateDataReq {
    pub base: BaseConf,
    pub ext: DataConf,
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
pub struct SearchDatasResp {
    pub total: usize,
    pub data: Vec<SearchDatasItemResp>,
}

#[derive(Serialize)]
pub struct SearchDatasItemResp {
    #[serde(flatten)]
    pub info: SearchDatasInfoResp,
    pub rule_ref: RuleRef,
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
pub struct SearchRuleInfo {
    pub databoard: SearchDataboardsItemResp,
    pub data: SearchDatasInfoResp,
}

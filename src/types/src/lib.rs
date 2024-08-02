use serde::{Deserialize, Serialize};

pub mod apps;
pub mod devices;
pub mod rules;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct BaseConf {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Pagination {
    #[serde(rename = "p")]
    pub page: usize,
    #[serde(rename = "s")]
    pub size: usize,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub typ: Option<String>,
    pub on: Option<bool>,
    pub err: Option<bool>,
}

#[derive(Deserialize)]
pub struct Value {
    pub value: serde_json::Value,
}

// 用于常量对比或从消息中取值
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct TargetValue {
    #[serde(rename = "type")]
    pub typ: TargetValueType,
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TargetValueType {
    Const,
    Variable,
}

#[derive(Serialize)]
pub struct Dashboard {
    pub device: DashboardDevice,
    pub app: DashboardApp,
    pub rule: DashboardRule,
}

#[derive(Serialize)]
pub struct DashboardDevice {
    pub total: usize,
    pub on_cnt: usize,
    pub err_cnt: usize,
}

#[derive(Serialize)]
pub struct DashboardApp {
    pub total: usize,
    pub on_cnt: usize,
    pub err_cnt: usize,
}

#[derive(Serialize)]
pub struct DashboardRule {
    pub total: usize,
    pub on_cnt: usize,
}

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

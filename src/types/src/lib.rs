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

#[derive(Serialize)]
pub struct RuleRef {
    pub rule_ref_cnt: usize,
    pub rule_active_ref_cnt: usize,
}

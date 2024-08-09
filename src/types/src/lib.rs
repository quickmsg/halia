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
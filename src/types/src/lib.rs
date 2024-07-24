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

#[derive(Deserialize)]
pub struct Value {
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SinkValue {
    #[serde(rename = "type")]
    pub typ: SinkValueType,
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SinkValueType {
    Const,
    Variable,
}

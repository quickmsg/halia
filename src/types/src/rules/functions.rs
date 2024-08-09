use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct WindowConf {
    #[serde(rename = "type")]
    pub typ: String,
    pub count: Option<u64>,
    // s
    pub interval: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FilterConf {
    pub filters: Vec<FilterConfItem>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FilterConfItem {
    #[serde(rename = "type")]
    pub typ: FilterType,
    pub field: String,
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum FilterType {
    Ct,
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Neq,
    Reg,
}

#[derive(Deserialize, Serialize)]
pub struct ComputerConf {
    pub computers: Vec<ComputerConfItem>,
}

#[derive(Deserialize, Serialize)]
pub struct ComputerConfItem {
    #[serde(rename = "type")]
    pub typ: ComputerType,
    pub name: String,
    pub field: String,
    pub target_field: Option<String>,
    pub arg: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputerType {
    Number,
    String,
    Hash,
    Date,
}

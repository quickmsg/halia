use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conf {
    #[serde(rename = "conf")]
    pub items: Vec<ItemConf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemConf {
    #[serde(rename = "type")]
    pub typ: Type,
    pub field: String,
    pub target_field: Option<String>,
    pub args: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Sum,
    Avg,
    Max,
    Min,
    Count,
    Collect,
    Merge,
    Deduplicate,
}

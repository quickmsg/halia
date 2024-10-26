use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Conf {
    pub items: Vec<ItemConf>,
}

#[derive(Deserialize, Serialize)]
pub struct ItemConf {
    #[serde(rename = "type")]
    pub typ: Type,
    pub name: String,
    pub field: String,
    pub target_field: Option<String>,
    pub arg: Option<Vec<serde_json::Value>>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Number,
    String,
    Hash,
    Date,
}

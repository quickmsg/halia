use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct Conf {
    #[serde(rename = "conf")]
    pub items: Vec<ItemConf>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ItemConf {
    #[serde(rename = "type")]
    pub typ: Type,
    pub field: String,
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Ct,
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Neq,
    Reg,
    IsArray,
}

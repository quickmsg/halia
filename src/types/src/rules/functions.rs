use serde::{Deserialize, Serialize};

use crate::TargetValue;

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
    #[serde(rename = "type")]
    pub typ: String,
    pub field: String,
    pub value: TargetValue,
}
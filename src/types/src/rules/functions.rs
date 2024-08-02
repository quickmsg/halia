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

#[derive(Deserialize, Serialize)]
pub struct ComputerConf {
    #[serde(rename = "type")]
    pub typ: ComputerType,
    pub field: String,
    pub target_field: Option<String>,
    pub arg: Option<TargetValue>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputerType {
    Abs,
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atan2,
    Atanh,
    Cbrt,
    Ceil,
    Cos,
    Cosh,
    Degrees,
    Exp,
    Exp2,
    Floor,
    Ln,
    Log,
    Sin,
}

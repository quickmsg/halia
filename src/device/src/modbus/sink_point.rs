use common::error::{HaliaError, HaliaResult};
use serde::{Deserialize, Serialize};
use types::device::datatype::DataType;

use super::group_point::Area;

#[derive(Debug, Clone)]
pub struct Point {
    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub value: TargetValue,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PointConf {
    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone)]
pub enum TargetValue {
    Int(i64),
    Uint(u64),
    Float(f64),
    Boolean(bool),
    String(String),
    Null,
    Field(String),
}

impl Point {
    pub fn new(data: &String) -> HaliaResult<Self> {
        let conf: PointConf = serde_json::from_str(data)?;
        let value = match &conf.value {
            serde_json::Value::Null => TargetValue::Null,
            serde_json::Value::Bool(b) => TargetValue::Boolean(*b),
            serde_json::Value::Number(n) => {
                if let Some(uint) = n.as_u64() {
                    TargetValue::Uint(uint)
                } else if let Some(int) = n.as_i64() {
                    TargetValue::Int(int)
                } else if let Some(float) = n.as_f64() {
                    TargetValue::Float(float)
                } else {
                    unreachable!()
                }
            }
            serde_json::Value::String(s) => {
                if s.starts_with("'") && s.ends_with("'") && s.len() >= 3 {
                    TargetValue::String(s.trim_start_matches("'").trim_end_matches("'").to_string())
                } else {
                    TargetValue::Field(s.clone())
                }
            }
            _ => return Err(HaliaError::ConfErr),
        };

        Ok(Point {
            r#type: conf.r#type.clone(),
            slave: conf.slave,
            area: conf.area.clone(),
            address: conf.address,
            value,
        })
    }
}

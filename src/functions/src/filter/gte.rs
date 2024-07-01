use anyhow::{bail, Result};
use message::{Message, MessageValue};
use serde::{Deserialize, Serialize};

use super::Filter;

pub struct Gte {
    field: String,
    value: TargetValue,
}

impl Gte {
    pub fn new(conf: serde_json::Value) -> Result<Self> {
        let conf: Conf = serde_json::from_value(conf)?;
        let value = match conf.value {
            serde_json::Value::Number(number) => {
                if let Some(int) = number.as_i64() {
                    TargetValue::Int(int)
                } else if let Some(float) = number.as_f64() {
                    TargetValue::Float(float)
                } else {
                    bail!("parse value failed")
                }
            }
            serde_json::Value::String(string) => {
                if string.starts_with("'") && string.ends_with("'") && string.len() >= 3 {
                    TargetValue::String(
                        string
                            .trim_start_matches("'")
                            .trim_end_matches("'")
                            .to_string(),
                    )
                } else {
                    TargetValue::Field(string)
                }
            }
            _ => bail!("not support"),
        };
        Ok(Self {
            field: conf.field,
            value,
        })
    }
}

#[derive(Deserialize, Serialize)]
struct Conf {
    field: String,
    value: serde_json::Value,
}

enum TargetValue {
    Int(i64),
    Float(f64),
    String(String),
    Field(String),
}

impl Filter for Gte {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(value) => match value {
                MessageValue::Int64(lhs) => match &self.value {
                    TargetValue::Int(rhs) => *lhs >= *rhs as i64,
                    TargetValue::Float(rhs) => *lhs >= *rhs as i64,
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int64(rhs) => lhs >= rhs,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as i64,
                            MessageValue::Float64(rhs) => *lhs >= *rhs as i64,
                            _ => false,
                        },
                        None => false,
                    },
                    TargetValue::String(_) => false,
                },
                MessageValue::Uint64(lhs) => match &self.value {
                    TargetValue::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs >= *rhs as u64
                    }
                    TargetValue::Float(rhs) => {
                        if *rhs < 0.0 {
                            return false;
                        }
                        *lhs as f64 >= *rhs
                    }
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u64
                            }
                            MessageValue::Uint64(rhs) => lhs >= rhs,
                            MessageValue::Float64(rhs) => {
                                // TODO 溢出问题
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f64 >= *rhs
                            }
                            _ => false,
                        },
                        None => false,
                    },
                    TargetValue::String(_) => false,
                },
              
                MessageValue::Float64(lhs) => match &self.value {
                    TargetValue::Int(rhs) => *lhs >= *rhs as f64,
                    TargetValue::Float(rhs) => *lhs >= *rhs,
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int64(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Float64(rhs) => lhs >= rhs,
                            _ => false,
                        },
                        None => false,
                    },
                    TargetValue::String(_) => false,
                },
                MessageValue::String(lhs) => match &self.value {
                    TargetValue::String(rhs) => lhs >= rhs,
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::String(rhs) => lhs >= rhs,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                _ => false,
            },
            None => false,
        }
    }
}

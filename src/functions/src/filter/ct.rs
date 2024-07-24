use anyhow::Result;
use message::{Message, MessageValue};
use serde::{Deserialize, Serialize};

use super::Filter;

struct Ct {
    field: String,
    value: TargetValue,
}

pub const TYPE: &str = "ct";

pub fn new(conf: serde_json::Value) -> Result<Box<dyn Filter>> {
    let conf: Conf = serde_json::from_value(conf)?;
    let value = match conf.value {
        serde_json::Value::Number(number) => {
            if let Some(uint) = number.as_u64() {
                TargetValue::Uint(uint)
            } else if let Some(int) = number.as_i64() {
                TargetValue::Int(int)
            } else if let Some(float) = number.as_f64() {
                TargetValue::Float(float)
            } else {
                unreachable!()
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
        serde_json::Value::Null => TargetValue::Null,
        serde_json::Value::Bool(bool) => TargetValue::Boolean(bool),
        serde_json::Value::Array(array) => TargetValue::Array(array),
        serde_json::Value::Object(obj) => TargetValue::Object(obj),
    };
    Ok(Box::new(Ct {
        field: conf.field,
        value,
    }))
}

#[derive(Deserialize, Serialize)]
struct Conf {
    field: String,
    value: serde_json::Value,
}

enum TargetValue {
    Int(i64),
    Uint(u64),
    Float(f64),
    Boolean(bool),
    String(String),
    Array(Vec<serde_json::Value>),
    Null,
    Object(serde_json::Map<String, serde_json::Value>),
    Field(String),
}

impl Filter for Ct {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(value) => match value {
                MessageValue::Array(values) => match &self.value {
                    TargetValue::Uint(rhs) => {
                        for item in values {
                            match item {
                                MessageValue::Int64(lhs) => {
                                    if *lhs > 0 && *lhs as u64 == *rhs {
                                        return true;
                                    }
                                }
                                MessageValue::Uint64(lhs) => {
                                    if *lhs == *rhs {
                                        return true;
                                    }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    TargetValue::Int(rhs) => {
                        for item in values {
                            match item {
                                MessageValue::Int64(lhs) => {
                                    if *lhs == *rhs {
                                        return true;
                                    }
                                }
                                MessageValue::Uint64(lhs) => {
                                    if *lhs <= i64::MAX as u64 && *lhs as i64 == *rhs {
                                        return true;
                                    }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    TargetValue::Float(rhs) => {
                        for item in values {
                            match item {
                                MessageValue::Float64(f64) => {
                                    if *f64 == *rhs {
                                        return true;
                                    }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    TargetValue::Boolean(rhs) => {
                        for item in values {
                            match item {
                                MessageValue::Boolean(bool) => {
                                    if *bool == *rhs {
                                        return true;
                                    }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    TargetValue::String(rhs) => {
                        for item in values {
                            match item {
                                MessageValue::String(string) => {
                                    if *string == *rhs {
                                        return true;
                                    }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    TargetValue::Null => {
                        for item in values {
                            match item {
                                MessageValue::Null => return true,
                                _ => {}
                            }
                        }
                        false
                    }
                    TargetValue::Array(_) => todo!(),
                    TargetValue::Object(_) => todo!(),
                    TargetValue::Field(field) => {
                        todo!()
                    }
                },
                _ => false,
            },
            None => false,
        }
    }
}

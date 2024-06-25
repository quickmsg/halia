use anyhow::{bail, Result};
use message::{MessageValue, Message};
use serde::{Deserialize, Serialize};

use super::Filter;

pub struct Ct {
    field: String,
    value: TargetValue,
}

impl Ct {
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
            serde_json::Value::Null => TargetValue::Null,
            serde_json::Value::Bool(bool) => TargetValue::Boolean(bool),
            serde_json::Value::Array(array) => TargetValue::Array(array),
            serde_json::Value::Object(obj) => TargetValue::Object(obj),
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
                    TargetValue::Int(rhs) => {
                        for item in values {
                            match item {
                                MessageValue::Int8(i8) => {
                                    if *i8 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                MessageValue::Int16(i16) => {
                                    if *i16 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                MessageValue::Int32(i32) => {
                                    if *i32 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                MessageValue::Int64(i64) => {
                                    if *i64 == *rhs {
                                        return true;
                                    }
                                }
                                MessageValue::Uint8(u8) => {
                                    if *u8 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                MessageValue::Uint16(u16) => {
                                    if *u16 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                MessageValue::Uint32(u32) => {
                                    if *u32 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                MessageValue::Uint64(u64) => {
                                    // TODO
                                    if *u64 as i64 == *rhs {
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
                                MessageValue::Float32(f32) => {
                                    if *f32 as f64 == *rhs {
                                        return true;
                                    }
                                }
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
                    TargetValue::Array(_) => todo!(),
                    TargetValue::Null => {
                        for item in values {
                            match item {
                                MessageValue::Null => return true,
                                _ => {}
                            }
                        }
                        false
                    }
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

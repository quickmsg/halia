use anyhow::{bail, Result};
use message::Message;
use serde::{Deserialize, Serialize};

use super::Filter;

pub struct Ct {
    field: String,
    value: Value,
}

impl Ct {
    pub fn new(conf: serde_json::Value) -> Result<Self> {
        let conf: Conf = serde_json::from_value(conf)?;
        let value = match conf.value {
            serde_json::Value::Number(number) => {
                if let Some(int) = number.as_i64() {
                    Value::Int(int)
                } else if let Some(float) = number.as_f64() {
                    Value::Float(float)
                } else {
                    bail!("parse value failed")
                }
            }
            serde_json::Value::String(string) => {
                if string.starts_with("'") && string.ends_with("'") && string.len() >= 3 {
                    Value::String(
                        string
                            .trim_start_matches("'")
                            .trim_end_matches("'")
                            .to_string(),
                    )
                } else {
                    Value::Field(string)
                }
            }
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(bool) => Value::Boolean(bool),
            serde_json::Value::Array(array) => Value::Array(array),
            serde_json::Value::Object(obj) => Value::Object(obj),
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

enum Value {
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
                json::Value::Array(values) => match &self.value {
                    Value::Int(rhs) => {
                        for item in values {
                            match item {
                                json::Value::Int8(i8) => {
                                    if *i8 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                json::Value::Int16(i16) => {
                                    if *i16 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                json::Value::Int32(i32) => {
                                    if *i32 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                json::Value::Int64(i64) => {
                                    if *i64 == *rhs {
                                        return true;
                                    }
                                }
                                json::Value::Uint8(u8) => {
                                    if *u8 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                json::Value::Uint16(u16) => {
                                    if *u16 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                json::Value::Uint32(u32) => {
                                    if *u32 as i64 == *rhs {
                                        return true;
                                    }
                                }
                                json::Value::Uint64(u64) => {
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
                    Value::Float(rhs) => {
                        for item in values {
                            match item {
                                json::Value::Float32(f32) => {
                                    if *f32 as f64 == *rhs {
                                        return true;
                                    }
                                }
                                json::Value::Float64(f64) => {
                                    if *f64 == *rhs {
                                        return true;
                                    }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    Value::Boolean(rhs) => {
                        for item in values {
                            match item {
                                json::Value::Boolean(bool) => {
                                    if *bool == *rhs {
                                        return true;
                                    }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    Value::String(rhs) => {
                        for item in values {
                            match item {
                                json::Value::String(string) => {
                                    if *string == *rhs {
                                        return true;
                                    }
                                }
                                _ => {}
                            }
                        }
                        false
                    }
                    Value::Array(_) => todo!(),
                    Value::Null => {
                        for item in values {
                            match item {
                                json::Value::Null => return true,
                                _ => {}
                            }
                        }
                        false
                    }
                    Value::Object(_) => todo!(),
                    Value::Field(field) => {
                        todo!()
                    }
                },
                _ => false,
            },
            None => false,
        }
    }
}

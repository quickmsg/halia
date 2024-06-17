use anyhow::{bail, Result};
use message::Message;
use serde::{Deserialize, Serialize};

use super::Filter;

pub struct Neq {
    field: String,
    value: Value,
}

impl Neq {
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

impl Filter for Neq {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(value) => match value {
                json::Value::Int8(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs == *rhs as i8,
                    Value::Float(rhs) => *lhs == *rhs as i8,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => lhs == rhs,
                            json::Value::Int16(rhs) => *lhs == *rhs as i8,
                            json::Value::Int32(rhs) => *lhs == *rhs as i8,
                            json::Value::Int64(rhs) => *lhs == *rhs as i8,
                            json::Value::Uint8(rhs) => *lhs == *rhs as i8,
                            json::Value::Uint16(rhs) => *lhs == *rhs as i8,
                            json::Value::Uint32(rhs) => *lhs == *rhs as i8,
                            json::Value::Uint64(rhs) => *lhs == *rhs as i8,
                            json::Value::Float32(rhs) => *lhs == *rhs as i8,
                            json::Value::Float64(rhs) => *lhs == *rhs as i8,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Int16(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs == *rhs as i16,
                    Value::Float(rhs) => *lhs == *rhs as i16,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => *lhs == *rhs as i16,
                            json::Value::Int16(rhs) => lhs == rhs,
                            json::Value::Int32(rhs) => *lhs == *rhs as i16,
                            json::Value::Int64(rhs) => *lhs == *rhs as i16,
                            json::Value::Uint8(rhs) => *lhs == *rhs as i16,
                            json::Value::Uint16(rhs) => *lhs == *rhs as i16,
                            json::Value::Uint32(rhs) => *lhs == *rhs as i16,
                            json::Value::Uint64(rhs) => *lhs == *rhs as i16,
                            json::Value::Float32(rhs) => *lhs == *rhs as i16,
                            json::Value::Float64(rhs) => *lhs == *rhs as i16,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Int32(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs == *rhs as i32,
                    Value::Float(rhs) => *lhs == *rhs as i32,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => *lhs == *rhs as i32,
                            json::Value::Int16(rhs) => *lhs == *rhs as i32,
                            json::Value::Int32(rhs) => lhs == rhs,
                            json::Value::Int64(rhs) => *lhs == *rhs as i32,
                            json::Value::Uint8(rhs) => *lhs == *rhs as i32,
                            json::Value::Uint16(rhs) => *lhs == *rhs as i32,
                            json::Value::Uint32(rhs) => *lhs == *rhs as i32,
                            json::Value::Uint64(rhs) => *lhs == *rhs as i32,
                            json::Value::Float32(rhs) => *lhs == *rhs as i32,
                            json::Value::Float64(rhs) => *lhs == *rhs as i32,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Int64(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs == *rhs as i64,
                    Value::Float(rhs) => *lhs == *rhs as i64,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => *lhs == *rhs as i64,
                            json::Value::Int16(rhs) => *lhs == *rhs as i64,
                            json::Value::Int32(rhs) => *lhs == *rhs as i64,
                            json::Value::Int64(rhs) => lhs == rhs,
                            json::Value::Uint8(rhs) => *lhs == *rhs as i64,
                            json::Value::Uint16(rhs) => *lhs == *rhs as i64,
                            json::Value::Uint32(rhs) => *lhs == *rhs as i64,
                            json::Value::Uint64(rhs) => *lhs == *rhs as i64,
                            json::Value::Float32(rhs) => *lhs == *rhs as i64,
                            json::Value::Float64(rhs) => *lhs == *rhs as i64,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Uint8(lhs) => match &self.value {
                    Value::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs == *rhs as u8
                    }
                    Value::Float(rhs) => {
                        if *rhs < 0.0 {
                            return false;
                        }
                        *lhs as f64 == *rhs
                    }
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u8
                            }
                            json::Value::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u8
                            }
                            json::Value::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u8
                            }
                            json::Value::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u8
                            }
                            json::Value::Uint8(rhs) => *lhs == *rhs as u8,
                            json::Value::Uint16(rhs) => *lhs == *rhs as u8,
                            json::Value::Uint32(rhs) => *lhs == *rhs as u8,
                            json::Value::Uint64(rhs) => *lhs == *rhs as u8,
                            json::Value::Float32(rhs) => {
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f32 == *rhs
                            }
                            json::Value::Float64(rhs) => {
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f64 == *rhs
                            }
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Uint16(lhs) => match &self.value {
                    Value::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs == *rhs as u16
                    }
                    Value::Float(rhs) => {
                        if *rhs < 0.0 {
                            return false;
                        }
                        *lhs as f64 == *rhs
                    }
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u16
                            }
                            json::Value::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u16
                            }
                            json::Value::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u16
                            }
                            json::Value::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u16
                            }
                            json::Value::Uint8(rhs) => *lhs == *rhs as u16,
                            json::Value::Uint16(rhs) => lhs == rhs,
                            json::Value::Uint32(rhs) => *lhs == *rhs as u16,
                            json::Value::Uint64(rhs) => *lhs == *rhs as u16,
                            json::Value::Float32(rhs) => *lhs == *rhs as u16,
                            json::Value::Float64(rhs) => *lhs == *rhs as u16,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Uint32(lhs) => match &self.value {
                    Value::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs == *rhs as u32
                    }
                    Value::Float(rhs) => {
                        if *rhs < 0.0 {
                            return false;
                        }
                        *lhs as f64 == *rhs
                    }
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u32
                            }
                            json::Value::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u32
                            }
                            json::Value::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u32
                            }
                            json::Value::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs == *rhs as u32
                            }
                            json::Value::Uint8(rhs) => *lhs == *rhs as u32,
                            json::Value::Uint16(rhs) => *lhs == *rhs as u32,
                            json::Value::Uint32(rhs) => lhs == rhs,
                            json::Value::Uint64(rhs) => *lhs == *rhs as u32,
                            json::Value::Float32(rhs) => {
                                // TODO 溢出问题
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f32 == *rhs
                            }
                            json::Value::Float64(rhs) => {
                                // TODO 溢出问题
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f64 > *rhs
                            }
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Uint64(lhs) => match &self.value {
                    Value::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs > *rhs as u64
                    }
                    Value::Float(rhs) => {
                        if *rhs < 0.0 {
                            return false;
                        }
                        *lhs as f64 > *rhs
                    }
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u64
                            }
                            json::Value::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u64
                            }
                            json::Value::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u64
                            }
                            json::Value::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u64
                            }
                            json::Value::Uint8(rhs) => *lhs > *rhs as u64,
                            json::Value::Uint16(rhs) => *lhs > *rhs as u64,
                            json::Value::Uint32(rhs) => *lhs > *rhs as u64,
                            json::Value::Uint64(rhs) => lhs > rhs,
                            json::Value::Float32(rhs) => {
                                // TODO 溢出问题
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f32 > *rhs
                            }
                            json::Value::Float64(rhs) => {
                                // TODO 溢出问题
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f64 > *rhs
                            }
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Float32(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs > *rhs as f32,
                    Value::Float(rhs) => *lhs > *rhs as f32,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => *lhs > *rhs as f32,
                            json::Value::Int16(rhs) => *lhs > *rhs as f32,
                            json::Value::Int32(rhs) => *lhs > *rhs as f32,
                            json::Value::Int64(rhs) => *lhs > *rhs as f32,
                            json::Value::Uint8(rhs) => *lhs > *rhs as f32,
                            json::Value::Uint16(rhs) => *lhs > *rhs as f32,
                            json::Value::Uint32(rhs) => *lhs > *rhs as f32,
                            json::Value::Uint64(rhs) => *lhs > *rhs as f32,
                            json::Value::Float32(rhs) => lhs > rhs,
                            json::Value::Float64(rhs) => *lhs > *rhs as f32,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Float64(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs > *rhs as f64,
                    Value::Float(rhs) => *lhs > *rhs,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => *lhs > *rhs as f64,
                            json::Value::Int16(rhs) => *lhs > *rhs as f64,
                            json::Value::Int32(rhs) => *lhs > *rhs as f64,
                            json::Value::Int64(rhs) => *lhs > *rhs as f64,
                            json::Value::Uint8(rhs) => *lhs > *rhs as f64,
                            json::Value::Uint16(rhs) => *lhs > *rhs as f64,
                            json::Value::Uint32(rhs) => *lhs > *rhs as f64,
                            json::Value::Uint64(rhs) => *lhs > *rhs as f64,
                            json::Value::Float32(rhs) => *lhs > *rhs as f64,
                            json::Value::Float64(rhs) => lhs > rhs,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::String(lhs) => match &self.value {
                    Value::String(rhs) => lhs == rhs,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::String(rhs) => lhs == rhs,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Null => match &self.value {
                    Value::Null => true,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Null => true,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Boolean(lhs) => match &self.value {
                    Value::Boolean(rhs) => lhs == rhs,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Boolean(rhs) => lhs == rhs,
                            _ => false,
                        },
                        None => false,
                    },
                    _ => false,
                },
                json::Value::Array(lhs) => match &self.value {
                    Value::Array(rhs) => {
                        if lhs.len() != rhs.len() {
                            return false;
                        }

                        // TODO 挨个比较

                        true
                    }
                    Value::Field(_) => todo!(),
                    _ => false,
                },
                json::Value::Object(_) => todo!(),
            },
            None => false,
        }
    }
}

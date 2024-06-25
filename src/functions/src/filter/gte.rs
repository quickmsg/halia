use anyhow::{bail, Result};
use message::{MessageValue, Message};
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
                MessageValue::Int8(lhs) => match &self.value {
                    TargetValue::Int(rhs) => *lhs >= *rhs as i8,
                    TargetValue::Float(rhs) => *lhs >= *rhs as i8,
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int8(rhs) => lhs >= rhs,
                            MessageValue::Int16(rhs) => *lhs >= *rhs as i8,
                            MessageValue::Int32(rhs) => *lhs >= *rhs as i8,
                            MessageValue::Int64(rhs) => *lhs >= *rhs as i8,
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as i8,
                            MessageValue::Uint16(rhs) => *lhs >= *rhs as i8,
                            MessageValue::Uint32(rhs) => *lhs >= *rhs as i8,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as i8,
                            MessageValue::Float32(rhs) => *lhs >= *rhs as i8,
                            MessageValue::Float64(rhs) => *lhs >= *rhs as i8,
                            _ => false,
                        },
                        None => false,
                    },
                    TargetValue::String(_) => false,
                },
                MessageValue::Int16(lhs) => match &self.value {
                    TargetValue::Int(rhs) => *lhs > *rhs as i16,
                    TargetValue::Float(rhs) => *lhs > *rhs as i16,
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int8(rhs) => *lhs >= *rhs as i16,
                            MessageValue::Int16(rhs) => lhs >= rhs,
                            MessageValue::Int32(rhs) => *lhs >= *rhs as i16,
                            MessageValue::Int64(rhs) => *lhs >= *rhs as i16,
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as i16,
                            MessageValue::Uint16(rhs) => *lhs >= *rhs as i16,
                            MessageValue::Uint32(rhs) => *lhs >= *rhs as i16,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as i16,
                            MessageValue::Float32(rhs) => *lhs >= *rhs as i16,
                            MessageValue::Float64(rhs) => *lhs >= *rhs as i16,
                            _ => false,
                        },
                        None => false,
                    },
                    TargetValue::String(_) => false,
                },
                MessageValue::Int32(lhs) => match &self.value {
                    TargetValue::Int(rhs) => *lhs > *rhs as i32,
                    TargetValue::Float(rhs) => *lhs > *rhs as i32,
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int8(rhs) => *lhs >= *rhs as i32,
                            MessageValue::Int16(rhs) => *lhs >= *rhs as i32,
                            MessageValue::Int32(rhs) => lhs >= rhs,
                            MessageValue::Int64(rhs) => *lhs >= *rhs as i32,
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as i32,
                            MessageValue::Uint16(rhs) => *lhs >= *rhs as i32,
                            MessageValue::Uint32(rhs) => *lhs >= *rhs as i32,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as i32,
                            MessageValue::Float32(rhs) => *lhs >= *rhs as i32,
                            MessageValue::Float64(rhs) => *lhs >= *rhs as i32,
                            _ => false,
                        },
                        None => false,
                    },
                    TargetValue::String(_) => false,
                },
                MessageValue::Int64(lhs) => match &self.value {
                    TargetValue::Int(rhs) => *lhs >= *rhs as i64,
                    TargetValue::Float(rhs) => *lhs >= *rhs as i64,
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int8(rhs) => *lhs >= *rhs as i64,
                            MessageValue::Int16(rhs) => *lhs >= *rhs as i64,
                            MessageValue::Int32(rhs) => *lhs >= *rhs as i64,
                            MessageValue::Int64(rhs) => lhs >= rhs,
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as i64,
                            MessageValue::Uint16(rhs) => *lhs >= *rhs as i64,
                            MessageValue::Uint32(rhs) => *lhs >= *rhs as i64,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as i64,
                            MessageValue::Float32(rhs) => *lhs >= *rhs as i64,
                            MessageValue::Float64(rhs) => *lhs >= *rhs as i64,
                            _ => false,
                        },
                        None => false,
                    },
                    TargetValue::String(_) => false,
                },
                MessageValue::Uint8(lhs) => match &self.value {
                    TargetValue::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs >= *rhs as u8
                    }
                    TargetValue::Float(rhs) => {
                        if *rhs < 0.0 {
                            return false;
                        }
                        *lhs as f64 >= *rhs
                    }
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int8(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u8
                            }
                            MessageValue::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u8
                            }
                            MessageValue::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u8
                            }
                            MessageValue::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u8
                            }
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as u8,
                            MessageValue::Uint16(rhs) => *lhs >= *rhs as u8,
                            MessageValue::Uint32(rhs) => *lhs >= *rhs as u8,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as u8,
                            MessageValue::Float32(rhs) => {
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f32 >= *rhs
                            }
                            MessageValue::Float64(rhs) => {
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
                MessageValue::Uint16(lhs) => match &self.value {
                    TargetValue::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs >= *rhs as u16
                    }
                    TargetValue::Float(rhs) => {
                        if *rhs < 0.0 {
                            return false;
                        }
                        *lhs as f64 >= *rhs
                    }
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int8(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u16
                            }
                            MessageValue::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u16
                            }
                            MessageValue::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u16
                            }
                            MessageValue::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u16
                            }
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as u16,
                            MessageValue::Uint16(rhs) => lhs >= rhs,
                            MessageValue::Uint32(rhs) => *lhs >= *rhs as u16,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as u16,
                            MessageValue::Float32(rhs) => *lhs >= *rhs as u16,
                            MessageValue::Float64(rhs) => *lhs >= *rhs as u16,
                            _ => false,
                        },
                        None => false,
                    },
                    TargetValue::String(_) => false,
                },
                MessageValue::Uint32(lhs) => match &self.value {
                    TargetValue::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs >= *rhs as u32
                    }
                    TargetValue::Float(rhs) => {
                        if *rhs < 0.0 {
                            return false;
                        }
                        *lhs as f64 >= *rhs
                    }
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int8(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u32
                            }
                            MessageValue::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u32
                            }
                            MessageValue::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u32
                            }
                            MessageValue::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u32
                            }
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as u32,
                            MessageValue::Uint16(rhs) => *lhs >= *rhs as u32,
                            MessageValue::Uint32(rhs) => lhs >= rhs,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as u32,
                            MessageValue::Float32(rhs) => {
                                // TODO 溢出问题
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f32 >= *rhs
                            }
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
                            MessageValue::Int8(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u64
                            }
                            MessageValue::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u64
                            }
                            MessageValue::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u64
                            }
                            MessageValue::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs >= *rhs as u64
                            }
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as u64,
                            MessageValue::Uint16(rhs) => *lhs >= *rhs as u64,
                            MessageValue::Uint32(rhs) => *lhs >= *rhs as u64,
                            MessageValue::Uint64(rhs) => lhs >= rhs,
                            MessageValue::Float32(rhs) => {
                                // TODO 溢出问题
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f32 >= *rhs
                            }
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
                MessageValue::Float32(lhs) => match &self.value {
                    TargetValue::Int(rhs) => *lhs >= *rhs as f32,
                    TargetValue::Float(rhs) => *lhs >= *rhs as f32,
                    TargetValue::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            MessageValue::Int8(rhs) => *lhs >= *rhs as f32,
                            MessageValue::Int16(rhs) => *lhs >= *rhs as f32,
                            MessageValue::Int32(rhs) => *lhs >= *rhs as f32,
                            MessageValue::Int64(rhs) => *lhs >= *rhs as f32,
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as f32,
                            MessageValue::Uint16(rhs) => *lhs >= *rhs as f32,
                            MessageValue::Uint32(rhs) => *lhs >= *rhs as f32,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as f32,
                            MessageValue::Float32(rhs) => lhs >= rhs,
                            MessageValue::Float64(rhs) => *lhs >= *rhs as f32,
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
                            MessageValue::Int8(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Int16(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Int32(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Int64(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Uint8(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Uint16(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Uint32(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Uint64(rhs) => *lhs >= *rhs as f64,
                            MessageValue::Float32(rhs) => *lhs >= *rhs as f64,
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

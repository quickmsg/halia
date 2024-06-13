use message::Message;

pub struct Gt {
    field: String,
    value: Value,
}

impl Gt {
    pub fn new() -> Self {
        Self {
            field: todo!(),
            value: todo!(),
        }
    }
}

enum Value {
    Int(i64),
    Float(f64),
    Field(String),
}

impl Gt {
    fn filter(&self, msg: &Message) -> bool {
        match msg.get(&self.field) {
            Some(value) => match value {
                json::Value::Int8(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs > *rhs as i8,
                    Value::Float(rhs) => *lhs > *rhs as i8,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => lhs > rhs,
                            json::Value::Int16(rhs) => *lhs > *rhs as i8,
                            json::Value::Int32(rhs) => *lhs > *rhs as i8,
                            json::Value::Int64(rhs) => *lhs > *rhs as i8,
                            json::Value::UInt8(rhs) => *lhs > *rhs as i8,
                            json::Value::UInt16(rhs) => *lhs > *rhs as i8,
                            json::Value::UInt32(rhs) => *lhs > *rhs as i8,
                            json::Value::UInt64(rhs) => *lhs > *rhs as i8,
                            json::Value::Float32(rhs) => *lhs > *rhs as i8,
                            json::Value::Float64(rhs) => *lhs > *rhs as i8,
                            _ => false,
                        },
                        None => false,
                    },
                },
                json::Value::Int16(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs > *rhs as i16,
                    Value::Float(rhs) => *lhs > *rhs as i16,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => *lhs > *rhs as i16,
                            json::Value::Int16(rhs) => lhs > rhs,
                            json::Value::Int32(rhs) => *lhs > *rhs as i16,
                            json::Value::Int64(rhs) => *lhs > *rhs as i16,
                            json::Value::UInt8(rhs) => *lhs > *rhs as i16,
                            json::Value::UInt16(rhs) => *lhs > *rhs as i16,
                            json::Value::UInt32(rhs) => *lhs > *rhs as i16,
                            json::Value::UInt64(rhs) => *lhs > *rhs as i16,
                            json::Value::Float32(rhs) => *lhs > *rhs as i16,
                            json::Value::Float64(rhs) => *lhs > *rhs as i16,
                            _ => false,
                        },
                        None => false,
                    },
                },
                json::Value::Int32(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs > *rhs as i32,
                    Value::Float(rhs) => *lhs > *rhs as i32,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => *lhs > *rhs as i32,
                            json::Value::Int16(rhs) => *lhs > *rhs as i32,
                            json::Value::Int32(rhs) => lhs > rhs,
                            json::Value::Int64(rhs) => *lhs > *rhs as i32,
                            json::Value::UInt8(rhs) => *lhs > *rhs as i32,
                            json::Value::UInt16(rhs) => *lhs > *rhs as i32,
                            json::Value::UInt32(rhs) => *lhs > *rhs as i32,
                            json::Value::UInt64(rhs) => *lhs > *rhs as i32,
                            json::Value::Float32(rhs) => *lhs > *rhs as i32,
                            json::Value::Float64(rhs) => *lhs > *rhs as i32,
                            _ => false,
                        },
                        None => false,
                    },
                },
                json::Value::Int64(lhs) => match &self.value {
                    Value::Int(rhs) => *lhs > *rhs as i64,
                    Value::Float(rhs) => *lhs > *rhs as i64,
                    Value::Field(field) => match msg.get(&field) {
                        Some(value) => match value {
                            json::Value::Int8(rhs) => *lhs > *rhs as i64,
                            json::Value::Int16(rhs) => *lhs > *rhs as i64,
                            json::Value::Int32(rhs) => *lhs > *rhs as i64,
                            json::Value::Int64(rhs) => lhs > rhs,
                            json::Value::UInt8(rhs) => *lhs > *rhs as i64,
                            json::Value::UInt16(rhs) => *lhs > *rhs as i64,
                            json::Value::UInt32(rhs) => *lhs > *rhs as i64,
                            json::Value::UInt64(rhs) => *lhs > *rhs as i64,
                            json::Value::Float32(rhs) => *lhs > *rhs as i64,
                            json::Value::Float64(rhs) => *lhs > *rhs as i64,
                            _ => false,
                        },
                        None => false,
                    },
                },
                json::Value::UInt8(lhs) => match &self.value {
                    Value::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs > *rhs as u8
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
                                *lhs > *rhs as u8
                            }
                            json::Value::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u8
                            }
                            json::Value::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u8
                            }
                            json::Value::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u8
                            }
                            json::Value::UInt8(rhs) => *lhs > *rhs as u8,
                            json::Value::UInt16(rhs) => *lhs > *rhs as u8,
                            json::Value::UInt32(rhs) => *lhs > *rhs as u8,
                            json::Value::UInt64(rhs) => *lhs > *rhs as u8,
                            json::Value::Float32(rhs) => {
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f32 > *rhs
                            }
                            json::Value::Float64(rhs) => {
                                if *rhs < 0.0 {
                                    return false;
                                }
                                *lhs as f64 > *rhs
                            }
                            _ => false,
                        },
                        None => false,
                    },
                },
                json::Value::UInt16(lhs) => match &self.value {
                    Value::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs > *rhs as u16
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
                                *lhs > *rhs as u16
                            }
                            json::Value::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u16
                            }
                            json::Value::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u16
                            }
                            json::Value::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u16
                            }
                            json::Value::UInt8(rhs) => *lhs > *rhs as u16,
                            json::Value::UInt16(rhs) => lhs > rhs,
                            json::Value::UInt32(rhs) => *lhs > *rhs as u16,
                            json::Value::UInt64(rhs) => *lhs > *rhs as u16,
                            json::Value::Float32(rhs) => *lhs > *rhs as u16,
                            json::Value::Float64(rhs) => *lhs > *rhs as u16,
                            _ => false,
                        },
                        None => false,
                    },
                },
                json::Value::UInt32(lhs) => match &self.value {
                    Value::Int(rhs) => {
                        if *rhs < 0 {
                            return false;
                        }
                        *lhs > *rhs as u32
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
                                *lhs > *rhs as u32
                            }
                            json::Value::Int16(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u32
                            }
                            json::Value::Int32(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u32
                            }
                            json::Value::Int64(rhs) => {
                                if *rhs < 0 {
                                    return false;
                                }
                                *lhs > *rhs as u32
                            }
                            json::Value::UInt8(rhs) => *lhs > *rhs as u32,
                            json::Value::UInt16(rhs) => *lhs > *rhs as u32,
                            json::Value::UInt32(rhs) => lhs > rhs,
                            json::Value::UInt64(rhs) => *lhs > *rhs as u32,
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
                },
                json::Value::UInt64(lhs) => match &self.value {
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
                            json::Value::UInt8(rhs) => *lhs > *rhs as u64,
                            json::Value::UInt16(rhs) => *lhs > *rhs as u64,
                            json::Value::UInt32(rhs) => *lhs > *rhs as u64,
                            json::Value::UInt64(rhs) => lhs > rhs,
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
                            json::Value::UInt8(rhs) => *lhs > *rhs as f32,
                            json::Value::UInt16(rhs) => *lhs > *rhs as f32,
                            json::Value::UInt32(rhs) => *lhs > *rhs as f32,
                            json::Value::UInt64(rhs) => *lhs > *rhs as f32,
                            json::Value::Float32(rhs) => lhs > rhs,
                            json::Value::Float64(rhs) => *lhs > *rhs as f32,
                            _ => false,
                        },
                        None => false,
                    },
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
                            json::Value::UInt8(rhs) => *lhs > *rhs as f64,
                            json::Value::UInt16(rhs) => *lhs > *rhs as f64,
                            json::Value::UInt32(rhs) => *lhs > *rhs as f64,
                            json::Value::UInt64(rhs) => *lhs > *rhs as f64,
                            json::Value::Float32(rhs) => *lhs > *rhs as f64,
                            json::Value::Float64(rhs) => lhs > rhs,
                            _ => false,
                        },
                        None => false,
                    },
                },
                _ => false,
            },
            None => false,
        }
    }
}

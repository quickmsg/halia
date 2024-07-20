use anyhow::{bail, Result};
use message::MessageValue;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::SinkValue;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateModbusReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub interval: u64,
    pub link_type: LinkType,
    pub ethernet: Option<Ethernet>,
    pub serial: Option<Serial>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Ethernet {
    pub mode: Mode,
    pub encode: Encode,
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Serial {
    pub path: String,
    pub stop_bits: u8,
    pub baud_rate: u32,
    pub data_bits: u8,
    pub parity: u8,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum LinkType {
    Ethernet,
    Serial,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Client,
    Server,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Encode {
    Tcp,
    RtuOverTcp,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdatePointReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub data_type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DataType {
    pub typ: Type,
    pub endian0: Option<Endian>,
    pub endian1: Option<Endian>,
    pub len: Option<u16>,
    pub single: Option<bool>,
    pub pos: Option<u8>,
}

impl DataType {
    pub fn get_quantity(&self) -> Option<u16> {
        match &self.typ {
            Type::Bool | Type::Int8 | Type::Uint8 | Type::Int16 | Type::Uint16 => Some(1),
            Type::Int32 | Type::Uint32 | Type::Float32 => Some(2),
            Type::Int64 | Type::Uint64 | Type::Float64 => Some(4),
            Type::String | Type::Bytes => self.len,
        }
    }

    pub fn decode(&self, data: &mut Vec<u8>) -> MessageValue {
        match self.typ {
            Type::Bool => match self.pos {
                Some(pos) => {
                    let data = match data.as_slice() {
                        [a, b] => [*a, *b],
                        _ => return MessageValue::Null,
                    };
                    let data = u16::from_be_bytes(data);
                    MessageValue::Boolean((data >> pos) & 1 != 0)
                }
                None => {
                    if data[0] == 1 {
                        MessageValue::Boolean(true)
                    } else {
                        MessageValue::Boolean(false)
                    }
                }
            },
            Type::Int8 => {
                let data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                match self.endian0.as_ref().unwrap() {
                    Endian::Little => MessageValue::Int64(data[0] as i64),
                    Endian::Big => MessageValue::Int64(data[1] as i64),
                }
            }
            Type::Uint8 => {
                let data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                match self.endian0.as_ref().unwrap() {
                    Endian::Little => MessageValue::Uint64(data[0] as u64),
                    Endian::Big => MessageValue::Uint64(data[1] as u64),
                }
            }
            Type::Int16 => {
                let mut data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                match self.endian0.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => data.swap(0, 1),
                }
                MessageValue::Int64(i16::from_be_bytes(data) as i64)
            }
            Type::Uint16 => {
                let mut data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                match self.endian0.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => data.swap(0, 1),
                }
                MessageValue::Uint64(u16::from_be_bytes(data) as u64)
            }
            Type::Int32 => {
                let mut data = match data.as_slice() {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return MessageValue::Null,
                };
                match self.endian0.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                    }
                }
                match self.endian1.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 2);
                        data.swap(1, 3);
                    }
                }
                MessageValue::Int64(i32::from_be_bytes(data) as i64)
            }
            Type::Uint32 => {
                let mut data = match data.as_slice() {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return MessageValue::Null,
                };
                match self.endian0.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                    }
                }
                match self.endian1.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 2);
                        data.swap(1, 3);
                    }
                }
                MessageValue::Uint64(u32::from_be_bytes(data) as u64)
            }
            Type::Int64 => {
                let mut data = match data.as_slice() {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return MessageValue::Null,
                };
                match self.endian0.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                        data.swap(4, 5);
                        data.swap(6, 7);
                    }
                }
                match self.endian1.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 6);
                        data.swap(1, 7);
                        data.swap(2, 4);
                        data.swap(3, 5);
                    }
                }
                MessageValue::Int64(i64::from_be_bytes(data))
            }
            Type::Uint64 => {
                let mut data = match data.as_slice() {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return MessageValue::Null,
                };
                match self.endian0.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                        data.swap(4, 5);
                        data.swap(6, 7);
                    }
                }
                match self.endian1.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 6);
                        data.swap(1, 7);
                        data.swap(2, 4);
                        data.swap(3, 5);
                    }
                }
                MessageValue::Uint64(u64::from_be_bytes(data))
            }
            Type::Float32 => {
                let mut data = match data.as_slice() {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return MessageValue::Null,
                };
                match self.endian0.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                    }
                }
                match self.endian1.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 2);
                        data.swap(1, 3);
                    }
                }
                MessageValue::Float64(f32::from_be_bytes(data) as f64)
            }
            // DataType::Float64(endian0, endian1) => {
            //     let mut data = match data.as_slice() {
            //         [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
            //         _ => return MessageValue::Null,
            //     };
            //     if *endian0 == Endian::LittleEndian {
            //         data.swap(0, 1);
            //         data.swap(2, 3);
            //         data.swap(4, 5);
            //         data.swap(6, 7);
            //     }
            //     if *endian1 == Endian::LittleEndian {
            //         data.swap(0, 6);
            //         data.swap(1, 7);
            //         data.swap(2, 4);
            //         data.swap(3, 5);
            //     }
            //     MessageValue::Float64(f64::from_be_bytes(data))
            // }
            // DataType::String(len, single, endian) => match (single, endian) {
            //     (true, Endian::BigEndian) => {
            //         let mut new_data = vec![];
            //         for i in 0..*len * 2 {
            //             if i % 2 != 0 {
            //                 new_data.push(data[i as usize]);
            //             }
            //         }
            //         match String::from_utf8(new_data) {
            //             Ok(string) => return MessageValue::String(string.replace("\0", "")),
            //             Err(_) => return MessageValue::Null,
            //         }
            //     }
            //     (true, Endian::LittleEndian) => {
            //         let mut new_data = vec![];
            //         for i in 0..*len * 2 {
            //             if i % 2 == 0 {
            //                 new_data.push(data[i as usize]);
            //             }
            //         }
            //         match String::from_utf8(new_data) {
            //             Ok(string) => return MessageValue::String(string.replace("\0", "")),
            //             Err(_) => return MessageValue::Null,
            //         }
            //     }
            //     (false, Endian::BigEndian) => {
            //         for i in 0..*len * 2 {
            //             if i % 2 == 0 {
            //                 data.swap((i * 2) as usize, (i * 2 + 1) as usize);
            //             }
            //         }
            //         match String::from_utf8(data.clone()) {
            //             Ok(string) => return MessageValue::String(string),
            //             Err(_) => return MessageValue::Null,
            //         }
            //     }
            //     (false, Endian::LittleEndian) => match String::from_utf8(data.clone()) {
            //         Ok(string) => return MessageValue::String(string),
            //         Err(_) => return MessageValue::Null,
            //     },
            // },
            // TODO
            // DataType::Bytes(_, _, _) => MessageValue::Bytes(data.clone()),
            _ => MessageValue::Null,
        }
    }

    pub fn encode(&self, value: serde_json::Value) -> Result<Vec<u8>> {
        match self.typ {
            Type::Bool => match value.as_bool() {
                Some(value) => {
                    let mut data = Vec::with_capacity(1);
                    if value {
                        data.push(1);
                    } else {
                        data.push(0);
                    }
                    Ok(data)
                }
                None => bail!("value is wrong"),
            },
            Type::Int8 => match value.as_i64() {
                Some(value) => {
                    let value: i8 = match value.try_into() {
                        Ok(value) => value,
                        Err(e) => bail!("value is wrong :{}", e),
                    };
                    Ok(value.to_be_bytes().to_vec())
                }
                None => bail!("value is wrong"),
            },
            Type::Uint8 => match value.as_i64() {
                Some(value) => {
                    let value: u8 = match value.try_into() {
                        Ok(value) => value,
                        Err(e) => bail!("value is wrong:{}", e),
                    };
                    Ok(value.to_be_bytes().to_vec())
                }
                None => bail!("value is wrong"),
            },
            Type::Int16 => match value.as_i64() {
                Some(value) => {
                    let value: i16 = match value.try_into() {
                        Ok(value) => value,
                        Err(e) => bail!("value is wrong:{}", e),
                    };
                    let data = match self.endian0.as_ref().unwrap() {
                        Endian::Little => value.to_be_bytes().to_vec(),
                        Endian::Big => value.to_le_bytes().to_vec(),
                    };
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            Type::Uint16 => match value.as_i64() {
                Some(value) => {
                    let value: u16 = match value.try_into() {
                        Ok(value) => value,
                        Err(e) => bail!("value is wrong:{}", e),
                    };
                    let data = match self.endian0.as_ref().unwrap() {
                        Endian::Little => value.to_be_bytes().to_vec(),
                        Endian::Big => value.to_le_bytes().to_vec(),
                    };
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            Type::Int32 => match value.as_i64() {
                Some(value) => {
                    let value: i32 = match value.try_into() {
                        Ok(value) => value,
                        Err(e) => bail!("value is wrong:{}", e),
                    };
                    let mut data = value.to_be_bytes();
                    match self.endian0.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 2);
                            data.swap(1, 3);
                        }
                    }
                    match self.endian1.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 1);
                            data.swap(2, 3)
                        }
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            Type::Uint32 => match value.as_i64() {
                Some(value) => {
                    let value: u32 = match value.try_into() {
                        Ok(value) => value,
                        Err(e) => bail!("value is wrong:{}", e),
                    };
                    let mut data = value.to_be_bytes();
                    match self.endian0.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 2);
                            data.swap(1, 3);
                        }
                    }
                    match self.endian1.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 1);
                            data.swap(2, 3)
                        }
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            // DataType::Int64(endian0, endian1) => match data.as_i64() {
            //     Some(value) => {
            //         let mut data = value.to_be_bytes();
            //         if *endian0 == Endian::BigEndian {
            //             data.swap(0, 6);
            //             data.swap(1, 7);
            //             data.swap(2, 4);
            //             data.swap(3, 5);
            //         }
            //         if *endian1 == Endian::BigEndian {
            //             data.swap(0, 1);
            //             data.swap(2, 3);
            //             data.swap(4, 5);
            //             data.swap(6, 7);
            //         }

            //         return Ok(data.to_vec());
            //     }
            //     None => bail!("value is wrong"),
            // },
            // DataType::Uint64(endian0, endian1) => match data.as_i64() {
            //     Some(value) => {
            //         let mut data = value.to_be_bytes();
            //         if *endian0 == Endian::BigEndian {
            //             data.swap(0, 6);
            //             data.swap(1, 7);
            //             data.swap(2, 4);
            //             data.swap(3, 5);
            //         }
            //         if *endian1 == Endian::BigEndian {
            //             data.swap(0, 1);
            //             data.swap(2, 3);
            //             data.swap(4, 5);
            //             data.swap(6, 7);
            //         }
            //         return Ok(data.to_vec());
            //     }
            //     None => bail!("value is wrong"),
            // },
            // DataType::Float32(endian0, endian1) => match data.as_f64() {
            //     Some(value) => {
            //         if value > f32::MAX as f64 {
            //             bail!("value is wrong, too big")
            //         };
            //         let mut data = (value as f32).to_be_bytes();
            //         if *endian0 == Endian::BigEndian {
            //             data.swap(0, 2);
            //             data.swap(1, 3);
            //         }
            //         if *endian1 == Endian::BigEndian {
            //             data.swap(0, 1);
            //             data.swap(2, 3)
            //         }
            //         return Ok(data.to_vec());
            //     }
            //     None => bail!("value is wrong"),
            // },
            // DataType::Float64(endian0, endian1) => match data.as_f64() {
            //     Some(value) => {
            //         let mut data = (value as f32).to_be_bytes();
            //         if *endian0 == Endian::BigEndian {
            //             data.swap(0, 6);
            //             data.swap(1, 7);
            //             data.swap(2, 4);
            //             data.swap(3, 5);
            //         }
            //         if *endian1 == Endian::BigEndian {
            //             data.swap(0, 1);
            //             data.swap(2, 3);
            //             data.swap(4, 5);
            //             data.swap(6, 7);
            //         }
            //         return Ok(data.to_vec());
            //     }
            //     None => bail!("value is wrong"),
            // },
            // DataType::String(len, single, endian) => match data.as_str() {
            //     Some(value) => {
            //         let mut new_data = vec![];
            //         let data = value.as_bytes();
            //         match single {
            //             true => {
            //                 if (*len as usize) < data.len() {
            //                     bail!("too long")
            //                 }
            //             }
            //             false => {
            //                 if (*len as usize) * 2 < data.len() {
            //                     bail!("too long")
            //                 }
            //             }
            //         }
            //         debug!("{:?}", data);
            //         match (single, endian) {
            //             (true, Endian::BigEndian) => {
            //                 for byte in data {
            //                     new_data.push(0);
            //                     new_data.push(*byte);
            //                 }
            //                 return Ok(new_data);
            //             }
            //             (true, Endian::LittleEndian) => {
            //                 for byte in data {
            //                     new_data.push(*byte);
            //                     new_data.push(0);
            //                 }
            //                 return Ok(new_data);
            //             }
            //             (false, Endian::BigEndian) => {
            //                 for i in 0..*len {
            //                     new_data.push(data[(i * 2 + 1) as usize]);
            //                     new_data.push(data[(i * 2) as usize]);
            //                 }
            //                 return Ok(data.to_vec());
            //             }
            //             (false, Endian::LittleEndian) => {
            //                 return Ok(data.to_vec());
            //             }
            //         }
            //     }
            //     None => bail!("value is wrong"),
            // },
            // DataType::Bytes(len, single, endian) => match data {
            //     Value::Array(arr) => {
            //         debug!("{arr:?}");
            //         match single {
            //             true => {
            //                 if (*len as usize) < arr.len() {
            //                     bail!("too long")
            //                 }
            //             }
            //             false => {
            //                 if (*len as usize) * 2 < arr.len() {
            //                     bail!("too long")
            //                 }
            //             }
            //         }

            //         let mut data = vec![];
            //         for v in arr {
            //             match v {
            //                 Value::Number(n) => match n.as_u64() {
            //                     Some(v) => data.push(v as u8),
            //                     None => bail!("not support"),
            //                 },
            //                 _ => bail!("not support"),
            //             }
            //         }
            //         let mut new_data = vec![];
            //         match (single, endian) {
            //             (true, Endian::BigEndian) => {
            //                 for byte in data {
            //                     new_data.push(0);
            //                     new_data.push(byte);
            //                 }
            //                 return Ok(new_data);
            //             }
            //             (true, Endian::LittleEndian) => {
            //                 for byte in data {
            //                     new_data.push(byte);
            //                     new_data.push(0);
            //                 }

            //                 return Ok(new_data);
            //             }
            //             (false, Endian::BigEndian) => {
            //                 for i in 0..*len {
            //                     new_data.push(data[(i * 2 + 1) as usize]);
            //                     new_data.push(data[(i * 2) as usize]);
            //                 }
            //                 return Ok(new_data);
            //             }
            //             (false, Endian::LittleEndian) => {
            //                 return Ok(data);
            //             }
            //         }
            //     }
            //     _ => bail!("not support"),
            // },
            _ => bail!("TODO"),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Bool,
    Int8,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Float32,
    Float64,
    String,
    Bytes,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Endian {
    Little,
    Big,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Area {
    InputDiscrete,
    Coils, // 可读写
    InputRegisters,
    HoldingRegisters, // 可读写
}

// impl TryFrom<u8> for Area {
//     type Error = ();

//     fn try_from(value: u8) -> Result<Self, Self::Error> {
//         match value {
//             1 => Ok(Area::InputDiscrete),
//             2 => Ok(Area::Coils),
//             3 => Ok(Area::InputRegisters),
//             4 => Ok(Area::HoldingRegisters),
//             _ => Err(()),
//         }
//     }
// }

#[derive(Serialize)]
pub struct SearchPointsResp {
    pub total: usize,
    pub data: Vec<SearchPointsItemResp>,
}

#[derive(Serialize)]
pub struct SearchPointsItemResp {
    pub id: Uuid,
    pub conf: CreateUpdatePointReq,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateSinkReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub r#type: DataType,
    pub slave: SinkValue,
    pub area: SinkValue,
    pub address: SinkValue,
    pub value: SinkValue,
}

#[derive(Serialize)]
pub struct SearchSinksResp {
    pub total: usize,
    pub data: Vec<SearchSinksItemResp>,
}

#[derive(Serialize)]
pub struct SearchSinksItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
}
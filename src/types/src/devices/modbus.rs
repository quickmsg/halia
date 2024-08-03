use anyhow::{bail, Result};
use message::MessageValue;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BaseConf, TargetValue};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateModbusReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: ModbusConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct ModbusConf {
    // ms
    pub interval: u64,
    // s
    pub reconnect: u64,
    pub link_type: LinkType,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub ethernet: Option<Ethernet>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
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
    pub stop_bits: StopBits,
    pub baud_rate: u32,
    pub data_bits: DataBits,
    pub parity: Parity,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StopBits {
    One,
    Two,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DataBits {
    Five,
    Six,
    Seven,
    Eight,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Parity {
    None,
    Odd,
    Even,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
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
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: PointConf,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct PointConf {
    #[serde(flatten)]
    pub data_type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct DataType {
    #[serde(rename = "type")]
    pub typ: Type,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single_endian: Option<Endian>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub double_endian: Option<Endian>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub len: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

    pub fn decode(&self, data: &mut [u8]) -> MessageValue {
        match self.typ {
            Type::Bool => match self.pos {
                Some(pos) => {
                    let data = match data {
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
                let data = match data {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => MessageValue::Int64(data[0] as i64),
                    Endian::Big => MessageValue::Int64(data[1] as i64),
                }
            }
            Type::Uint8 => {
                let data = match data {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => MessageValue::Uint64(data[0] as u64),
                    Endian::Big => MessageValue::Uint64(data[1] as u64),
                }
            }
            Type::Int16 => {
                let mut data = match data {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => data.swap(0, 1),
                }
                MessageValue::Int64(i16::from_be_bytes(data) as i64)
            }
            Type::Uint16 => {
                let mut data = match data {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => data.swap(0, 1),
                }
                MessageValue::Uint64(u16::from_be_bytes(data) as u64)
            }
            Type::Int32 => {
                let mut data = match data {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                    }
                }
                match self.double_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 2);
                        data.swap(1, 3);
                    }
                }
                MessageValue::Int64(i32::from_be_bytes(data) as i64)
            }
            Type::Uint32 => {
                let mut data = match data {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                    }
                }
                match self.double_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 2);
                        data.swap(1, 3);
                    }
                }
                MessageValue::Uint64(u32::from_be_bytes(data) as u64)
            }
            Type::Int64 => {
                let mut data = match data {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                        data.swap(4, 5);
                        data.swap(6, 7);
                    }
                }
                match self.double_endian.as_ref().unwrap() {
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
                let mut data = match data {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                        data.swap(4, 5);
                        data.swap(6, 7);
                    }
                }
                match self.double_endian.as_ref().unwrap() {
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
                let mut data = match data {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                    }
                }
                match self.double_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 2);
                        data.swap(1, 3);
                    }
                }
                MessageValue::Float64(f32::from_be_bytes(data) as f64)
            }
            Type::Float64 => {
                let mut data = match data {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return MessageValue::Null,
                };
                match self.single_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 1);
                        data.swap(2, 3);
                        data.swap(4, 5);
                        data.swap(6, 7);
                    }
                }
                match self.double_endian.as_ref().unwrap() {
                    Endian::Little => {}
                    Endian::Big => {
                        data.swap(0, 6);
                        data.swap(1, 7);
                        data.swap(2, 4);
                        data.swap(3, 5);
                    }
                }
                MessageValue::Float64(f64::from_be_bytes(data))
            }
            // Type::String => match (
            //     self.single.as_ref().unwrap(),
            //     self.single_endian.as_ref().unwrap(),
            // ) {
            //     (true, Endian::Big) => {
            //         let mut new_data = vec![];
            //         for i in 0..*self.len.as_ref().unwrap() * 2 {
            //             if i % 2 != 0 {
            //                 new_data.push(data[i as usize]);
            //             }
            //         }
            //         match String::from_utf8(new_data) {
            //             Ok(string) => return MessageValue::String(string.replace("\0", "")),
            //             Err(_) => return MessageValue::Null,
            //         }
            //     }
            //     (true, Endian::Little) => {
            //         let mut new_data = vec![];
            //         for i in 0..*self.len.as_ref().unwrap() * 2 {
            //             if i % 2 == 0 {
            //                 new_data.push(data[i as usize]);
            //             }
            //         }
            //         match String::from_utf8(new_data) {
            //             Ok(string) => return MessageValue::String(string.replace("\0", "")),
            //             Err(_) => return MessageValue::Null,
            //         }
            //     }
            //     (false, Endian::Big) => {
            //         for i in 0..*self.len.as_ref().unwrap() * 2 {
            //             if i % 2 == 0 {
            //                 data.swap((i * 2) as usize, (i * 2 + 1) as usize);
            //             }
            //         }
            //         match String::from_utf8(data) {
            //             Ok(string) => return MessageValue::String(string),
            //             Err(_) => return MessageValue::Null,
            //         }
            //     }
            //     (false, Endian::Little) => match String::from_utf8(data.clone()) {
            //         Ok(string) => return MessageValue::String(string),
            //         Err(_) => return MessageValue::Null,
            //     },
            // },
            // Type::Bytes => MessageValue::Bytes(data.clone()),
            _ => todo!(),
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
                    let data = match self.single_endian.as_ref().unwrap() {
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
                    let data = match self.single_endian.as_ref().unwrap() {
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
                    match self.single_endian.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 2);
                            data.swap(1, 3);
                        }
                    }
                    match self.double_endian.as_ref().unwrap() {
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
                    match self.single_endian.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 2);
                            data.swap(1, 3);
                        }
                    }
                    match self.double_endian.as_ref().unwrap() {
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
            Type::Int64 => match value.as_i64() {
                Some(value) => {
                    let mut data = value.to_be_bytes();
                    match self.single_endian.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 6);
                            data.swap(1, 7);
                            data.swap(2, 4);
                            data.swap(3, 5);
                        }
                    }
                    match self.double_endian.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 1);
                            data.swap(2, 3);
                            data.swap(4, 5);
                            data.swap(6, 7);
                        }
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            Type::Uint64 => match value.as_i64() {
                Some(value) => {
                    let mut data = value.to_be_bytes();
                    match self.single_endian.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 6);
                            data.swap(1, 7);
                            data.swap(2, 4);
                            data.swap(3, 5);
                        }
                    }
                    match self.double_endian.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 1);
                            data.swap(2, 3);
                            data.swap(4, 5);
                            data.swap(6, 7);
                        }
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            Type::Float32 => match value.as_f64() {
                Some(value) => {
                    if value > f32::MAX as f64 {
                        bail!("value is wrong, too big")
                    };
                    let mut data = (value as f32).to_be_bytes();
                    match self.single_endian.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 2);
                            data.swap(1, 3);
                        }
                    }
                    match self.double_endian.as_ref().unwrap() {
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
            Type::Float64 => match value.as_f64() {
                Some(value) => {
                    let mut data = (value as f32).to_be_bytes();
                    match self.single_endian.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 6);
                            data.swap(1, 7);
                            data.swap(2, 4);
                            data.swap(3, 5);
                        }
                    }
                    match self.double_endian.as_ref().unwrap() {
                        Endian::Little => {}
                        Endian::Big => {
                            data.swap(0, 1);
                            data.swap(2, 3);
                            data.swap(4, 5);
                            data.swap(6, 7);
                        }
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            Type::String => match value.as_str() {
                Some(value) => {
                    let mut new_data = vec![];
                    let data = value.as_bytes();
                    match self.single.as_ref().unwrap() {
                        true => {
                            if (*self.len.as_ref().unwrap() as usize) < data.len() {
                                bail!("too long")
                            }
                        }
                        false => {
                            if (*self.len.as_ref().unwrap() as usize) * 2 < data.len() {
                                bail!("too long")
                            }
                        }
                    }
                    match (
                        self.single.as_ref().unwrap(),
                        self.single_endian.as_ref().unwrap(),
                    ) {
                        (true, Endian::Big) => {
                            for byte in data {
                                new_data.push(0);
                                new_data.push(*byte);
                            }
                            return Ok(new_data);
                        }
                        (true, Endian::Little) => {
                            for byte in data {
                                new_data.push(*byte);
                                new_data.push(0);
                            }
                            return Ok(new_data);
                        }
                        (false, Endian::Big) => {
                            for i in 0..*self.len.as_ref().unwrap() {
                                new_data.push(data[(i * 2 + 1) as usize]);
                                new_data.push(data[(i * 2) as usize]);
                            }
                            return Ok(data.to_vec());
                        }
                        (false, Endian::Little) => {
                            return Ok(data.to_vec());
                        }
                    }
                }
                None => bail!("value is wrong"),
            },
            Type::Bytes => match value {
                serde_json::Value::Array(arr) => {
                    let mut data = vec![];
                    for v in arr {
                        match v {
                            serde_json::Value::Number(n) => match n.as_u64() {
                                Some(v) => data.push(v as u8),
                                None => bail!("not support"),
                            },
                            _ => bail!("not support"),
                        }
                    }

                    Ok(data)
                }
                _ => bail!("not support"),
            },
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
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

impl TryFrom<&str> for Type {
    type Error = ();

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "bool" => Ok(Type::Bool),
            "int8" => Ok(Type::Int8),
            "uint8" => Ok(Type::Uint8),
            "int16" => Ok(Type::Int16),
            "uint16" => Ok(Type::Uint16),
            "int32" => Ok(Type::Int32),
            "uint32" => Ok(Type::Uint32),
            "int64" => Ok(Type::Int64),
            "uint64" => Ok(Type::Uint64),
            "float32" => Ok(Type::Float32),
            "float64" => Ok(Type::Float64),
            "string" => Ok(Type::String),
            "bytes" => Ok(Type::Bytes),
            _ => Err(()),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Endian {
    Little,
    Big,
}

#[derive(Deserialize, Serialize, Debug, Copy, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Area {
    DiscretesInput,
    Coils, // 可读写
    InputRegisters,
    HoldingRegisters, // 可读写
}

impl TryFrom<&str> for Area {
    type Error = ();

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "input_discrete" => Ok(Area::DiscretesInput),
            "coils" => Ok(Area::Coils),
            "input_registers" => Ok(Area::InputRegisters),
            "holding_registers" => Ok(Area::HoldingRegisters),
            _ => Err(()),
        }
    }
}

#[derive(Serialize)]
pub struct SearchPointsResp {
    pub total: usize,
    pub data: Vec<SearchPointsItemResp>,
}

#[derive(Serialize)]
pub struct SearchPointsItemResp {
    pub id: Uuid,
    pub conf: CreateUpdatePointReq,
    pub value: serde_json::Value,
    pub err_info: Option<String>,
    pub active_ref_rule_cnt: usize,
    pub ref_rule_cnt: usize,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateSinkReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub sink: SinkConf,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SinkConf {
    #[serde(rename = "type")]
    pub typ: TargetValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single_endian: Option<TargetValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub double_endian: Option<TargetValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub len: Option<TargetValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single: Option<TargetValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pos: Option<TargetValue>,

    pub slave: TargetValue,
    pub area: TargetValue,
    pub address: TargetValue,
    pub value: TargetValue,
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

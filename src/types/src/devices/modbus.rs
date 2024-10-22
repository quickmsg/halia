use anyhow::{bail, Result};
use base64::{prelude::BASE64_STANDARD, Engine as _};
use message::MessageValue;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::MessageRetain;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Conf {
    pub link_type: LinkType,

    // ms
    pub interval: u64,
    // s
    pub reconnect: u64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub ethernet: Option<Ethernet>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub serial: Option<Serial>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LinkType {
    Ethernet,
    Serial,
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

#[derive(Deserialize_repr, Serialize_repr, Debug, Clone, PartialEq)]
#[repr(u8)]
pub enum StopBits {
    One = 1,
    Two = 2,
}

#[derive(Deserialize_repr, Serialize_repr, Debug, Clone, PartialEq)]
#[repr(u8)]
pub enum DataBits {
    Five = 5,
    Six = 6,
    Seven = 7,
    Eight = 8,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Parity {
    None,
    Odd,
    Even,
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

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SourceConf {
    // 字段名称
    pub field: String,
    #[serde(flatten)]
    pub data_type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SourceCustomizeConf {
    pub slave: u8,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SourceTemplateConf {
    // 字段名称
    pub field: String,
    #[serde(flatten)]
    pub data_type: DataType,
    // pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SinkTemplateConf {
    // todo
}

#[derive(Deserialize, Serialize, Debug, Copy, Clone, PartialEq)]
pub struct DataType {
    #[serde(rename = "type")]
    pub typ: Type,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single_endian: Option<Endian>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub double_endian: Option<Endian>,

    // string和bytes拥有len，值为寄存器数量
    #[serde(skip_serializing_if = "Option::is_none")]
    pub len: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pos: Option<u8>,
}

impl DataType {
    pub fn get_quantity(&self) -> u16 {
        match &self.typ {
            Type::Bool | Type::Int8 | Type::Uint8 | Type::Int16 | Type::Uint16 => 1,
            Type::Int32 | Type::Uint32 | Type::Float32 => 2,
            Type::Int64 | Type::Uint64 | Type::Float64 => 4,
            Type::String | Type::Bytes => *self.len.as_ref().unwrap(),
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
                    if pos <= 7 {
                        MessageValue::Boolean(((data[0] >> (7 - pos)) & 1) != 0)
                    } else {
                        MessageValue::Boolean(((data[1] >> (15 - pos)) & 1) != 0)
                    }
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
                    Endian::Little => MessageValue::Int64(data[0] as i64),
                    Endian::Big => MessageValue::Int64(data[1] as i64),
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
                MessageValue::Int64(u16::from_be_bytes(data) as i64)
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
                MessageValue::Int64(u32::from_be_bytes(data) as i64)
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
                MessageValue::Int64(u64::from_be_bytes(data) as i64)
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
            Type::String => match (
                self.single.as_ref().unwrap(),
                self.single_endian.as_ref().unwrap(),
            ) {
                (true, Endian::Big) => {
                    let mut new_data = vec![];
                    for (i, v) in data.iter().enumerate() {
                        if i % 2 != 0 {
                            new_data.push(*v);
                        }
                    }
                    match String::from_utf8(new_data) {
                        Ok(string) => return MessageValue::String(string.replace("\0", "")),
                        Err(_) => return MessageValue::Null,
                    }
                }
                (true, Endian::Little) => {
                    let mut new_data = vec![];
                    for (i, v) in data.iter().enumerate() {
                        if i % 2 == 0 {
                            new_data.push(*v);
                        }
                    }
                    match String::from_utf8(new_data) {
                        Ok(string) => return MessageValue::String(string.replace("\0", "")),
                        Err(_) => return MessageValue::Null,
                    }
                }
                (false, Endian::Big) => {
                    for i in 0..*self.len.as_ref().unwrap() {
                        data.swap((i * 2) as usize, (i * 2 + 1) as usize);
                    }
                    let new_data = data.to_vec();
                    match String::from_utf8(new_data) {
                        Ok(string) => return MessageValue::String(string),
                        Err(_) => return MessageValue::Null,
                    }
                }
                (false, Endian::Little) => {
                    let new_data = data.to_vec();
                    match String::from_utf8(new_data) {
                        Ok(string) => return MessageValue::String(string),
                        Err(_) => return MessageValue::Null,
                    }
                }
            },
            Type::Bytes => MessageValue::Bytes(data.to_vec()),
        }
    }

    pub fn encode(&self, value: serde_json::Value) -> Result<Vec<u8>> {
        match self.typ {
            Type::Bool => match value.as_bool() {
                Some(value) => match self.pos {
                    Some(pos) => {
                        let mut data = Vec::with_capacity(4);
                        if pos <= 7 {
                            data.push(!(1 << (7 - pos)));
                            data.push(0xFF);

                            match value {
                                true => {
                                    data.push(1 << (7 - pos));
                                    data.push(0x00);
                                }
                                false => {
                                    data.push(0x00);
                                    data.push(0x00);
                                }
                            }
                        } else {
                            data.push(0xFF);
                            data.push(!(1 << (15 - pos)));
                            match value {
                                true => {
                                    data.push(0x00);
                                    data.push(1 << (15 - pos));
                                }
                                false => {
                                    data.push(0x00);
                                    data.push(0x00);
                                }
                            }
                        }

                        Ok(data)
                    }
                    None => {
                        let mut data = Vec::with_capacity(2);
                        match value {
                            true => {
                                data.push(0xFF);
                                data.push(0x00);
                            }
                            false => {
                                data.push(0x00);
                                data.push(0x00);
                            }
                        }
                        Ok(data)
                    }
                },
                None => bail!("value is wrong"),
            },
            Type::Int8 => match value.as_i64() {
                Some(value) => {
                    let mut data = Vec::with_capacity(4);
                    match &self.single_endian.as_ref().unwrap() {
                        Endian::Little => {
                            data.push(0x00);
                            data.push(0xFF);

                            data.push(value as u8);
                            data.push(0x00);
                        }
                        Endian::Big => {
                            data.push(0xFF);
                            data.push(0x00);

                            data.push(0x00);
                            data.push(value as u8);
                        }
                    }
                    Ok(data)
                }
                None => bail!("value is wrong"),
            },
            Type::Uint8 => match value.as_i64() {
                Some(value) => {
                    let mut data = Vec::with_capacity(4);
                    match &self.single_endian.as_ref().unwrap() {
                        Endian::Little => {
                            data.push(0x00);
                            data.push(0xFF);

                            data.push(value as u8);
                            data.push(0x00);
                        }
                        Endian::Big => {
                            data.push(0xFF);
                            data.push(0x00);

                            data.push(0x00);
                            data.push(value as u8);
                        }
                    }
                    Ok(data)
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
                    let mut data = value.to_be_bytes();
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
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            Type::String => match value.as_str() {
                Some(value) => {
                    let mut new_data = vec![0; *self.len.as_ref().unwrap() as usize];
                    let data = value.as_bytes();
                    match self.single.as_ref().unwrap() {
                        true => {
                            if (*self.len.as_ref().unwrap() as usize) < data.len() * 2 {
                                bail!("too long")
                            }
                        }
                        false => {
                            if (*self.len.as_ref().unwrap() as usize) < data.len() {
                                bail!("too long")
                            }
                        }
                    }
                    match (
                        self.single.as_ref().unwrap(),
                        self.single_endian.as_ref().unwrap(),
                    ) {
                        (true, Endian::Big) => {
                            for (i, byte) in data.iter().enumerate() {
                                new_data[i * 2 + 1] = *byte;
                            }
                            return Ok(new_data);
                        }
                        (true, Endian::Little) => {
                            for (i, byte) in data.iter().enumerate() {
                                new_data[i * 2] = *byte;
                            }
                            return Ok(new_data);
                        }
                        (false, Endian::Big) => {
                            for (i, v) in data.iter().enumerate() {
                                match i % 2 {
                                    0 => {
                                        new_data[i + 1] = *v;
                                    }
                                    1 => {
                                        new_data[i - 1] = *v;
                                    }
                                    _ => unreachable!(),
                                }
                            }
                            return Ok(data.to_vec());
                        }
                        (false, Endian::Little) => {
                            for (i, v) in data.iter().enumerate() {
                                new_data[i] = *v;
                            }
                            return Ok(new_data);
                        }
                    }
                }
                None => bail!("value is wrong"),
            },
            Type::Bytes => match value {
                serde_json::Value::String(str) => match BASE64_STANDARD.decode(str) {
                    Ok(data) => {
                        if data.len() > (*self.len.as_ref().unwrap() as usize) {
                            bail!("值太长");
                        }
                        let mut new_data = vec![0; *self.len.as_ref().unwrap() as usize];
                        for (i, v) in data.iter().enumerate() {
                            new_data[i] = *v;
                        }

                        Ok(new_data)
                    }
                    Err(e) => bail!(e),
                },
                serde_json::Value::Array(array) => {
                    let mut data = vec![];
                    for item in array.iter() {
                        match item {
                            serde_json::Value::Number(n) => match n.as_u64() {
                                Some(n) => {
                                    if n > 255 {
                                        bail!("数字大于255")
                                    }
                                    data.push(n as u8);
                                }
                                None => bail!("数据含有非数字"),
                            },
                            _ => bail!("数据含有非number数字"),
                        }
                    }

                    Ok(data)
                }
                // serde_json::Value::Object(_) => todo!(),
                _ => bail!("不支持的类型"),
            },
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq)]
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

#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Endian {
    Little,
    Big,
}

// 每个区域可以有65536个数据
#[derive(Deserialize, Serialize, Debug, Copy, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Area {
    InputDiscrete,    // bit 只读
    Coils,            // bit 读写
    InputRegisters,   // 16-bit word 只读
    HoldingRegisters, // 16-bit word 读写
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SinkConf {
    #[serde(flatten)]
    pub data_type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub value: serde_json::Value,
    pub message_retain: MessageRetain,
}

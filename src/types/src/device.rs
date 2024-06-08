use anyhow::{bail, Result};
use serde::{de::value, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::{backtrace, str::FromStr};
use tracing::warn;
use uuid::Uuid;

#[derive(Deserialize, Debug, Serialize)]
pub struct CreateDeviceReq {
    pub r#type: String,
    pub name: String,
    pub conf: Value,
}

#[derive(Deserialize, Serialize)]
pub struct UpdateDeviceReq {
    pub name: String,
    pub conf: Value,
}

#[derive(Serialize)]
pub struct DeviceDetailResp {
    pub id: Uuid,
    pub r#type: &'static str,
    pub name: String,
    pub conf: Value,
}

#[derive(Serialize)]
pub struct ListDevicesResp {
    pub id: Uuid,
    pub name: String,
    pub r#type: &'static str,
    pub on: bool,
    pub err: bool,
    pub rtt: u16,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CreateGroupReq {
    pub name: String,
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct UpdateGroupReq {
    pub name: String,
    pub interval: u64,
}

#[derive(Serialize)]
pub struct ListGroupsResp {
    pub id: Uuid,
    pub name: String,
    pub point_count: u8,
    pub interval: u64,
}

#[derive(Serialize)]
pub struct ListPointResp {
    pub id: Uuid,
    pub name: String,
    pub address: u16,
    pub r#type: String,
    pub value: Value,
    pub describe: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreatePointReq {
    pub name: String,
    pub conf: Value,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WritePointValueReq {
    pub value: Value,
}

#[derive(Debug)]
pub enum DataType {
    Bool,
    Int16(Endian),
    Uint16(Endian),
    Int32(Endian, Endian),
    Uint32(Endian, Endian),
    Int64(Endian, Endian, Endian, Endian),
    Uint64(Endian, Endian, Endian, Endian),
    Float32(Endian, Endian),
    Float64(Endian, Endian, Endian, Endian),
    // TODO
    String,
    Bytes,
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: Value = Deserialize::deserialize(deserializer)?;

        match value {
            Value::Object(map) => {
                let type_value = map
                    .get("type")
                    .ok_or_else(|| serde::de::Error::missing_field("type"))?;
                let type_str = type_value.as_str().ok_or_else(|| {
                    serde::de::Error::invalid_type(
                        serde::de::Unexpected::Other("not a string"),
                        &"string",
                    )
                })?;

                let data_type = match type_str {
                    "bool" => DataType::Bool,
                    "int16" => {
                        // TODO remove unwrap
                        let endian = extract_endian(&map, 1).unwrap();
                        DataType::Int16(endian[0])
                    }
                    "uint16" => {
                        let endian = extract_endian(&map, 1).unwrap();
                        DataType::Uint16(endian[0])
                    }
                    "int32" => {
                        let endian = extract_endian(&map, 2).unwrap();
                        DataType::Int32(endian[0], endian[1])
                    }
                    "uint32" => {
                        let endian = extract_endian(&map, 2).unwrap();
                        DataType::Uint32(endian[0], endian[1])
                    }
                    "int64" => {
                        let endian = extract_endian(&map, 4).unwrap();
                        DataType::Int64(endian[0], endian[1], endian[2], endian[3])
                    }
                    "uint64" => {
                        let endian = extract_endian(&map, 4).unwrap();
                        DataType::Uint64(endian[0], endian[1], endian[2], endian[3])
                    }
                    "float32" => {
                        let endian = extract_endian(&map, 2).unwrap();
                        DataType::Float32(endian[0], endian[1])
                    }
                    "float64" => {
                        let endian = extract_endian(&map, 4).unwrap();
                        DataType::Float64(endian[0], endian[1], endian[2], endian[3])
                    }
                    "string" => DataType::String,
                    "bytes" => DataType::Bytes,
                    _ => {
                        return Err(serde::de::Error::unknown_variant(
                            type_str,
                            &[
                                "int16", "uint16", "int32", "uint32", "int64", "uint64", "float32",
                                "float64", "string", "bytes",
                            ],
                        ))
                    }
                };

                Ok(data_type)
            }
            _ => Err(serde::de::Error::invalid_type(
                serde::de::Unexpected::Other("not an object"),
                &"object",
            )),
        }
    }
}

impl Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            DataType::Bool => serde_json::json!({"type": "bool"}).serialize(serializer),
            DataType::Int16(endian) => {
                serde_json::json!({"type": "int16", "endian": [endian]}).serialize(serializer)
            }
            DataType::Uint16(endian) => {
                serde_json::json!({"type": "uint16", "endian": [endian]}).serialize(serializer)
            }
            DataType::Int32(endian1, endian2) => {
                serde_json::json!({"type": "int32", "endian": [endian1, endian2]})
                    .serialize(serializer)
            }
            DataType::Uint32(endian1, endian2) => {
                serde_json::json!({"type": "uint32", "endian": [endian1, endian2]})
                    .serialize(serializer)
            }
            DataType::Int64(endian1, endian2, endian3, endian4) => {
                serde_json::json!({"type": "int32", "endian": [endian1, endian2, endian3, endian4]})
                    .serialize(serializer)
            }
            DataType::Uint64(endian1, endian2, endian3, endian4) => {
                serde_json::json!({"type": "int32", "endian": [endian1, endian2, endian3, endian4]})
                    .serialize(serializer)
            }
            DataType::Float32(endian1, endian2) => {
                serde_json::json!({"type": "int32", "endian": [endian1, endian2]})
                    .serialize(serializer)
            }
            DataType::Float64(endian1, endian2, endian3, endian4) => {
                serde_json::json!({"type": "int32", "endian": [endian1, endian2, endian3, endian4]})
                    .serialize(serializer)
            }
            DataType::Bytes => serde_json::json!({"type": "bytes"}).serialize(serializer),
            DataType::String => serde_json::json!({"type": "string"}).serialize(serializer),
        }
    }
}

impl DataType {
    pub fn decode(&self, data: Vec<u8>) -> Value {
        match self {
            DataType::Bool => {
                if data.len() != 1 {
                    warn!("buf is not right");
                    Value::Null
                } else {
                    if data[0] == 1 {
                        Value::from(true)
                    } else {
                        Value::from(false)
                    }
                }
            }
            DataType::Int16(endian) => {
                let mut data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return Value::Null,
                };
                if *endian == Endian::LittleEndian {
                    data.swap(0, 1);
                }
                Value::from(i16::from_be_bytes(data))
            }
            DataType::Uint16(endian) => {
                let mut data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return Value::Null,
                };
                if *endian == Endian::LittleEndian {
                    data.swap(0, 1);
                }
                Value::from(i16::from_be_bytes(data))
            }
            DataType::Int32(endian0, endian1) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return Value::Null,
                };
                if *endian0 == Endian::LittleEndian {
                    data.swap(0, 1);
                }
                if *endian1 == Endian::LittleEndian {
                    data.swap(2, 3);
                }
                Value::from(i32::from_be_bytes(data))
            }
            DataType::Uint32(endian0, endian1) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return Value::Null,
                };
                if *endian0 == Endian::LittleEndian {
                    data.swap(0, 1);
                }
                if *endian1 == Endian::LittleEndian {
                    data.swap(2, 3);
                }
                Value::from(u32::from_be_bytes(data))
            }
            DataType::Int64(endian0, endian1, endian2, endian3) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return Value::Null,
                };
                if *endian0 == Endian::LittleEndian {
                    data.swap(0, 1);
                }
                if *endian1 == Endian::LittleEndian {
                    data.swap(2, 3);
                }
                if *endian2 == Endian::LittleEndian {
                    data.swap(4, 5);
                }
                if *endian3 == Endian::LittleEndian {
                    data.swap(6, 7);
                }

                Value::from(i64::from_be_bytes(data))
            }
            DataType::Uint64(endian0, endian1, endian2, endian3) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return Value::Null,
                };
                if *endian0 == Endian::LittleEndian {
                    data.swap(0, 1);
                }
                if *endian1 == Endian::LittleEndian {
                    data.swap(2, 3);
                }
                if *endian2 == Endian::LittleEndian {
                    data.swap(4, 5);
                }
                if *endian3 == Endian::LittleEndian {
                    data.swap(6, 7);
                }
                Value::from(u64::from_be_bytes(data))
            }
            DataType::Float32(endian0, endian1) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return Value::Null,
                };
                if *endian0 == Endian::LittleEndian {
                    data.swap(0, 1);
                }
                if *endian1 == Endian::LittleEndian {
                    data.swap(2, 3);
                }
                Value::from(f32::from_be_bytes(data))
            }
            DataType::Float64(endian0, endian1, endian2, endian3) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return Value::Null,
                };
                if *endian0 == Endian::LittleEndian {
                    data.swap(0, 1);
                }
                if *endian1 == Endian::LittleEndian {
                    data.swap(2, 3);
                }
                if *endian2 == Endian::LittleEndian {
                    data.swap(4, 5);
                }
                if *endian3 == Endian::LittleEndian {
                    data.swap(6, 7);
                }
                Value::from(f64::from_be_bytes(data))
            }
            // DataType::String => todo!(),
            // DataType::Bytes => todo!(),
            _ => Value::Null,
        }
    }

    pub fn encode(&self, data: Value) -> Result<Vec<u16>> {
        match self {
            DataType::Int16(endian) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as i16).to_be_bytes();
                    if *endian == Endian::LittleEndian {
                        data.swap(0, 1);
                    }
                    return Ok(data
                        .chunks(2)
                        .map(|chunk| ((chunk[0] as u16) << 8) | (chunk[1] as u16))
                        .collect());
                }
                None => bail!("value is wrong"),
            },
            DataType::Uint16(endian) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as u16).to_be_bytes();
                    if *endian == Endian::LittleEndian {
                        data.swap(0, 1);
                    }
                    return Ok(data
                        .chunks(2)
                        .map(|chunk| ((chunk[0] as u16) << 8) | (chunk[1] as u16))
                        .collect());
                }
                None => bail!("value is wrong"),
            },
            DataType::Int32(endian0, endian1) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as i32).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 1);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(2, 3);
                    }
                    return Ok(data
                        .chunks(2)
                        .map(|chunk| ((chunk[0] as u16) << 8) | (chunk[1] as u16))
                        .collect());
                }
                None => bail!("value is wrong"),
            },
            DataType::Uint32(endian0, endian1) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as u32).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 1);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(2, 3);
                    }
                    return Ok(data
                        .chunks(2)
                        .map(|chunk| ((chunk[0] as u16) << 8) | (chunk[1] as u16))
                        .collect());
                }
                None => bail!("value is wrong"),
            },
            DataType::Int64(endian0, endian1, endian2, endian3) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as i64).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 1);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(2, 3);
                    }
                    if *endian2 == Endian::LittleEndian {
                        data.swap(4, 5);
                    }
                    if *endian3 == Endian::LittleEndian {
                        data.swap(6, 7);
                    }
                    return Ok(data
                        .chunks(2)
                        .map(|chunk| ((chunk[0] as u16) << 8) | (chunk[1] as u16))
                        .collect());
                }
                None => bail!("value is wrong"),
            },
            DataType::Uint64(endian0, endian1, endian2, endian3) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as u64).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 1);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(2, 3);
                    }
                    if *endian2 == Endian::LittleEndian {
                        data.swap(4, 5);
                    }
                    if *endian3 == Endian::LittleEndian {
                        data.swap(6, 7);
                    }
                    return Ok(data
                        .chunks(2)
                        .map(|chunk| ((chunk[0] as u16) << 8) | (chunk[1] as u16))
                        .collect());
                }
                None => bail!("value is wrong"),
            },
            DataType::Float32(endian0, endian1) => match data.as_f64() {
                Some(value) => {
                    let mut data = (value as f32).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 1);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(2, 3);
                    }
                    return Ok(data
                        .chunks(2)
                        .map(|chunk| ((chunk[0] as u16) << 8) | (chunk[1] as u16))
                        .collect());
                }
                None => bail!("value is wrong"),
            },
            DataType::Float64(endian0, endian1, endian2, endian3) => match data.as_f64() {
                Some(value) => {
                    let mut data = (value as f32).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 1);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(2, 3);
                    }
                    if *endian2 == Endian::LittleEndian {
                        data.swap(4, 5);
                    }
                    if *endian3 == Endian::LittleEndian {
                        data.swap(6, 7);
                    }
                    return Ok(data
                        .chunks(2)
                        .map(|chunk| ((chunk[0] as u16) << 8) | (chunk[1] as u16))
                        .collect());
                }
                None => bail!("value is wrong"),
            },
            // DataType::String => todo!(),
            // DataType::Bytes => todo!(),
            _ => bail!("value is wrong"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Endian {
    BigEndian,
    LittleEndian,
}

impl FromStr for Endian {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "big_endian" => Ok(Endian::BigEndian),
            "little_endian" => Ok(Endian::LittleEndian),
            _ => Err(format!("Unknown Endian variant: {}", s)),
        }
    }
}

impl Serialize for Endian {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let endian_str = match self {
            Endian::BigEndian => "big_endian",
            Endian::LittleEndian => "little_endian",
        };
        serializer.serialize_str(endian_str)
    }
}

fn extract_endian(
    map: &serde_json::Map<String, Value>,
    count: usize,
) -> Result<Vec<Endian>, serde_json::Error> {
    let endian_value = map
        .get("endian")
        .ok_or_else(|| serde::de::Error::missing_field("endian"))?;
    let endian_array = endian_value.as_array().ok_or_else(|| {
        serde::de::Error::invalid_type(serde::de::Unexpected::Other("not an array"), &"array")
    })?;

    if endian_array.len() != count {
        return Err(serde::de::Error::invalid_length(
            endian_array.len(),
            &format!("{}", count).as_str(),
        ));
    }

    endian_array
        .iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| {
                    serde::de::Error::invalid_type(
                        serde::de::Unexpected::Other("not a string"),
                        &"string",
                    )
                })
                .and_then(|s| Endian::from_str(s).map_err(serde::de::Error::custom))
        })
        .collect()
}

#[derive(Deserialize, Serialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Client,
    Server,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Init,
    Runing,
    Paused,
    Error,
    Stoped,
}

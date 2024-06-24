use std::str::FromStr;

use anyhow::{bail, Result};
use message::value::MessageValue;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum DataType {
    Bool(u8),
    Int8(Endian),
    Uint8(Endian),
    Int16(Endian),
    Uint16(Endian),
    Int32(Endian, Endian),
    Uint32(Endian, Endian),
    Int64(Endian, Endian),
    Uint64(Endian, Endian),
    Float32(Endian, Endian),
    Float64(Endian, Endian),
    String(u16, bool, Endian),
    Bytes(u16),
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
                    "bool" => {
                        let pos = map
                            .get("len")
                            .ok_or_else(|| serde::de::Error::missing_field("len"))?;
                        let pos = pos.as_i64().ok_or_else(|| {
                            serde::de::Error::invalid_type(
                                serde::de::Unexpected::Other("not an array"),
                                &"array",
                            )
                        })?;
                        DataType::Bool(pos as u8)
                    }
                    "int8" => {
                        let endian = extract_endian(&map, 1).unwrap();
                        DataType::Int8(endian[0])
                    }
                    "uint8" => {
                        let endian = extract_endian(&map, 1).unwrap();
                        DataType::Uint8(endian[0])
                    }
                    "int16" => {
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
                        let endian = extract_endian(&map, 2).unwrap();
                        DataType::Int64(endian[0], endian[1])
                    }
                    "uint64" => {
                        let endian = extract_endian(&map, 2).unwrap();
                        DataType::Uint64(endian[0], endian[1])
                    }
                    "float32" => {
                        let endian = extract_endian(&map, 2).unwrap();
                        DataType::Float32(endian[0], endian[1])
                    }
                    "float64" => {
                        let endian = extract_endian(&map, 2).unwrap();
                        DataType::Float64(endian[0], endian[1])
                    }
                    "string" => {
                        let len = map
                            .get("len")
                            .ok_or_else(|| serde::de::Error::missing_field("len"))?;
                        let len = len.as_i64().ok_or_else(|| {
                            serde::de::Error::invalid_type(
                                serde::de::Unexpected::Other("not an array"),
                                &"array",
                            )
                        })?;

                        let single = map
                            .get("single")
                            .ok_or_else(|| serde::de::Error::missing_field("single"))?;
                        let single = single.as_bool().ok_or_else(|| {
                            serde::de::Error::invalid_type(
                                serde::de::Unexpected::Other("not an array"),
                                &"array",
                            )
                        })?;
                        let endian = extract_endian(&map, 1).unwrap();

                        DataType::String(len as u16, single, endian[0])
                    }
                    "bytes" => {
                        let len = map
                            .get("len")
                            .ok_or_else(|| serde::de::Error::missing_field("len"))?;
                        let len = len.as_i64().ok_or_else(|| {
                            serde::de::Error::invalid_type(
                                serde::de::Unexpected::Other("not an array"),
                                &"array",
                            )
                        })?;
                        DataType::Bytes(len as u16)
                    }
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
            DataType::Bool(pos) => serde_json::json!({"type": "bool", "pos": pos}).serialize(serializer),
            DataType::Int8(endian) => serde_json::json!({"type": "int8", "endian": [endian]}).serialize(serializer),
            DataType::Uint8(endian) =>  serde_json::json!({"type": "uint8", "endian": [endian]}).serialize(serializer),
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
            DataType::Int64(endian1, endian2) => {
                serde_json::json!({"type": "int32", "endian": [endian1, endian2]})
                    .serialize(serializer)
            }
            DataType::Uint64(endian1, endian2) => {
                serde_json::json!({"type": "int32", "endian": [endian1, endian2]})
                    .serialize(serializer)
            }
            DataType::Float32(endian1, endian2) => {
                serde_json::json!({"type": "int32", "endian": [endian1, endian2]})
                    .serialize(serializer)
            }
            DataType::Float64(endian1, endian2) => {
                serde_json::json!({"type": "int32", "endian": [endian1, endian2]})
                    .serialize(serializer)
            }
            DataType::Bytes(len) => {
                serde_json::json!({"type": "bytes", "len": len}).serialize(serializer)
            }
            DataType::String(len, single, endian ) => {
                serde_json::json!({"type": "string", "len": len, "single": single,  "endian": [endian]}).serialize(serializer)
            }
        }
    }
}

impl DataType {
    pub fn get_quantity(&self) -> u16 {
        match &self {
            DataType::Bool(_)
            | DataType::Int8(_)
            | DataType::Uint8(_)
            | DataType::Int16(_)
            | DataType::Uint16(_) => 1,
            DataType::Int32(_, _) | DataType::Uint32(_, _) | DataType::Float32(_, _) => 2,
            DataType::Int64(_, _) | DataType::Uint64(_, _) | DataType::Float64(_, _) => 4,
            DataType::String(len, _, _) => *len,
            DataType::Bytes(len) => *len,
        }
    }

    pub fn decode(&self, data: &mut Vec<u8>) -> MessageValue {
        match self {
            DataType::Bool(_) => {
                if data.len() != 1 {
                    MessageValue::Null
                } else {
                    if data[0] == 1 {
                        MessageValue::Boolean(true)
                    } else {
                        MessageValue::Boolean(false)
                    }
                }
            }
            DataType::Int8(endian) => {
                let data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                if *endian == Endian::LittleEndian {
                    MessageValue::Int8(data[0] as i8)
                } else {
                    MessageValue::Int8(data[1] as i8)
                }
            }
            DataType::Uint8(endian) => {
                let data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                if *endian == Endian::LittleEndian {
                    MessageValue::Uint8(data[0] as u8)
                } else {
                    MessageValue::Uint8(data[1] as u8)
                }
            }
            DataType::Int16(endian) => {
                let mut data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                if *endian == Endian::BigEndian {
                    data.swap(0, 1);
                }
                MessageValue::Int16(i16::from_be_bytes(data))
            }
            DataType::Uint16(endian) => {
                let mut data = match data.as_slice() {
                    [a, b] => [*a, *b],
                    _ => return MessageValue::Null,
                };
                if *endian == Endian::BigEndian {
                    data.swap(0, 1);
                }
                MessageValue::Uint16(u16::from_be_bytes(data))
            }
            DataType::Int32(endian0, endian1) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return MessageValue::Null,
                };
                if *endian0 == Endian::BigEndian {
                    data.swap(0, 1);
                    data.swap(2, 3);
                }
                if *endian1 == Endian::BigEndian {
                    data.swap(0, 2);
                    data.swap(1, 3);
                }
                MessageValue::Int32(i32::from_be_bytes(data))
            }
            DataType::Uint32(endian0, endian1) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return MessageValue::Null,
                };
                if *endian0 == Endian::BigEndian {
                    data.swap(0, 1);
                    data.swap(2, 3);
                }
                if *endian1 == Endian::BigEndian {
                    data.swap(0, 2);
                    data.swap(1, 3);
                }
                MessageValue::Uint32(u32::from_be_bytes(data))
            }
            DataType::Int64(endian0, endian1) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return MessageValue::Null,
                };
                if *endian0 == Endian::BigEndian {
                    data.swap(0, 1);
                    data.swap(2, 3);
                    data.swap(4, 5);
                    data.swap(6, 7);
                }
                if *endian1 == Endian::BigEndian {
                    data.swap(0, 6);
                    data.swap(1, 7);
                    data.swap(2, 4);
                    data.swap(3, 5);
                }
                MessageValue::Int64(i64::from_be_bytes(data))
            }
            DataType::Uint64(endian0, endian1) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return MessageValue::Null,
                };
                if *endian0 == Endian::BigEndian {
                    data.swap(0, 1);
                    data.swap(2, 3);
                    data.swap(4, 5);
                    data.swap(6, 7);
                }
                if *endian1 == Endian::BigEndian {
                    data.swap(0, 6);
                    data.swap(1, 7);
                    data.swap(2, 4);
                    data.swap(3, 5);
                }
                MessageValue::Uint64(u64::from_be_bytes(data))
            }
            DataType::Float32(endian0, endian1) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d] => [*a, *b, *c, *d],
                    _ => return MessageValue::Null,
                };
                if *endian0 == Endian::LittleEndian {
                    data.swap(0, 1);
                    data.swap(2, 3);
                }
                if *endian1 == Endian::LittleEndian {
                    data.swap(0, 2);
                    data.swap(1, 3);
                }
                MessageValue::Float32(f32::from_be_bytes(data))
            }
            DataType::Float64(endian0, endian1) => {
                let mut data = match data.as_slice() {
                    [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                    _ => return MessageValue::Null,
                };
                if *endian0 == Endian::LittleEndian {
                    data.swap(0, 1);
                    data.swap(2, 3);
                    data.swap(4, 5);
                    data.swap(6, 7);
                }
                if *endian1 == Endian::LittleEndian {
                    data.swap(0, 6);
                    data.swap(1, 7);
                    data.swap(2, 4);
                    data.swap(3, 5);
                }
                MessageValue::Float64(f64::from_be_bytes(data))
            }
            DataType::String(len, single, endian) => match (single, endian) {
                (true, Endian::BigEndian) => {
                    let mut new_data = vec![];
                    for i in 0..*len {
                        if i % 2 != 0 {
                            new_data.push(data[i as usize]);
                        }
                    }
                    match String::from_utf8(new_data) {
                        Ok(string) => return MessageValue::String(string),
                        Err(_) => return MessageValue::Null,
                    }
                }
                (true, Endian::LittleEndian) => {
                    let mut new_data = vec![];
                    for i in 0..*len {
                        if i % 2 == 0 {
                            new_data.push(data[i as usize]);
                        }
                    }
                    match String::from_utf8(new_data) {
                        Ok(string) => return MessageValue::String(string.replace("\0", "")),
                        Err(_) => return MessageValue::Null,
                    }
                }
                (false, Endian::BigEndian) => {
                    for i in 0..*len {
                        if i % 2 == 0 {
                            data.swap((i * 2) as usize, (i * 2 + 1) as usize);
                        }
                    }
                    match String::from_utf8(data.clone()) {
                        Ok(string) => return MessageValue::String(string),
                        Err(_) => return MessageValue::Null,
                    }
                }
                (false, Endian::LittleEndian) => match String::from_utf8(data.clone()) {
                    Ok(string) => return MessageValue::String(string),
                    Err(_) => return MessageValue::Null,
                },
            },
            DataType::Bytes(_) => MessageValue::Bytes(data.clone()),
        }
    }

    pub fn encode(&self, data: Value) -> Result<Vec<u8>> {
        match self {
            DataType::Bool(_) => match data.as_bool() {
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
            DataType::Int8(_) => match data.as_i64() {
                Some(value) => Ok((value as u8).to_be_bytes().to_vec()),
                None => bail!("value is wrong"),
            },
            DataType::Uint8(_) => match data.as_i64() {
                Some(value) => Ok((value as u8).to_be_bytes().to_vec()),
                None => bail!("value is wrong"),
            },
            DataType::Int16(endian) => match data.as_i64() {
                Some(value) => {
                    let data = match endian {
                        Endian::BigEndian => (value as i16).to_be_bytes().to_vec(),
                        Endian::LittleEndian => (value as i16).to_le_bytes().to_vec(),
                    };
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            DataType::Uint16(endian) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as u16).to_be_bytes();
                    if *endian == Endian::LittleEndian {
                        data.swap(0, 1);
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            DataType::Int32(endian0, endian1) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as i32).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 2);
                        data.swap(1, 3);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(0, 1);
                        data.swap(2, 3)
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            DataType::Uint32(endian0, endian1) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as u32).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 2);
                        data.swap(1, 3);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(0, 1);
                        data.swap(2, 3)
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            DataType::Int64(endian0, endian1) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as i64).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 6);
                        data.swap(1, 7);
                        data.swap(2, 4);
                        data.swap(3, 5);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(0, 1);
                        data.swap(2, 3);
                        data.swap(4, 5);
                        data.swap(6, 7);
                    }

                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            DataType::Uint64(endian0, endian1) => match data.as_i64() {
                Some(value) => {
                    let mut data = (value as u64).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 6);
                        data.swap(1, 7);
                        data.swap(2, 4);
                        data.swap(3, 5);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(0, 1);
                        data.swap(2, 3);
                        data.swap(4, 5);
                        data.swap(6, 7);
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            DataType::Float32(endian0, endian1) => match data.as_f64() {
                Some(value) => {
                    let mut data = (value as f32).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 2);
                        data.swap(1, 3);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(0, 1);
                        data.swap(2, 3)
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            DataType::Float64(endian0, endian1) => match data.as_f64() {
                Some(value) => {
                    let mut data = (value as f32).to_be_bytes();
                    if *endian0 == Endian::LittleEndian {
                        data.swap(0, 6);
                        data.swap(1, 7);
                        data.swap(2, 4);
                        data.swap(3, 5);
                    }
                    if *endian1 == Endian::LittleEndian {
                        data.swap(0, 1);
                        data.swap(2, 3);
                        data.swap(4, 5);
                        data.swap(6, 7);
                    }
                    return Ok(data.to_vec());
                }
                None => bail!("value is wrong"),
            },
            DataType::String(len, single, endian) => match data.as_str() {
                Some(value) => {
                    let mut new_data = vec![];
                    let data = value.as_bytes();
                    match (single, endian) {
                        (true, Endian::BigEndian) => {
                            for i in 0..*len {
                                new_data.push(0);
                                new_data.push(data[i as usize]);
                            }
                            return Ok(new_data);
                        }
                        (true, Endian::LittleEndian) => {
                            for i in 0..*len {
                                new_data.push(data[i as usize]);
                                new_data.push(0);
                            }
                            return Ok(new_data);
                        }
                        (false, Endian::BigEndian) => {
                            for i in 0..*len {
                                new_data.push(data[(i * 2 + 1) as usize]);
                                new_data.push(data[(i * 2) as usize]);
                            }
                            return Ok(data.to_vec());
                        }
                        (false, Endian::LittleEndian) => {
                            return Ok(data.to_vec());
                        }
                    }
                }
                None => bail!("value is wrong"),
            },
            DataType::Bytes(_) => todo!(),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            DataType::Bool(_) => "bool".to_string(),
            DataType::Int8(_) => "int8".to_string(),
            DataType::Uint8(_) => "uint8".to_string(),
            DataType::Int16(_) => "int16".to_string(),
            DataType::Uint16(_) => "uint16".to_string(),
            DataType::Int32(_, _) => "int32".to_string(),
            DataType::Uint32(_, _) => "uint32".to_string(),
            DataType::Int64(_, _) => "int64".to_string(),
            DataType::Uint64(_, _) => "uint64".to_string(),
            DataType::Float32(_, _) => "float32".to_string(),
            DataType::Float64(_, _) => "float64".to_string(),
            DataType::String(_, _, _) => "string".to_string(),
            DataType::Bytes(_) => "bytes".to_string(),
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

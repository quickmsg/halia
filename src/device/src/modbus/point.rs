use common::error::Result;
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use types::device::{CreatePointReq, DataType, Endian::LittleEndian};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct Point {
    pub id: Uuid,
    pub name: String,
    pub r#type: DataType,
    pub slave: u8,
    pub area: u8,
    pub address: u16,
    pub describe: Option<String>,
    pub quantity: u16,
    pub value: Value,
}

#[derive(Deserialize, Debug)]
pub(crate) struct PointConf {
    pub name: String,
    pub r#type: DataType,
    pub slave: u8,
    pub area: u8,
    pub address: u16,
    pub describe: Option<String>,
}

#[derive(Deserialize)]
struct UpdateConf {
    pub name: Option<String>,
    pub r#type: Option<DataType>,
    pub slave: Option<u8>,
    pub area: Option<u8>,
    pub address: Option<u16>,
    pub describe: Option<String>,
}

impl Point {
    pub fn new(conf: CreatePointReq, id: Uuid) -> Result<Point> {
        let conf: PointConf = serde_json::from_value(conf)?;
        let quantity = match conf.r#type {
            DataType::Bool => 1,
            DataType::Int16(_) => 1,
            DataType::Uint16(_) => 1,
            DataType::Int32(_, _) => 2,
            DataType::Uint32(_, _) => 2,
            DataType::Int64(_, _, _, _) => 4,
            DataType::Uint64(_, _, _, _) => 4,
            DataType::Float32(_, _) => 2,
            DataType::Float64(_, _, _, _) => 4,
            DataType::String => todo!(),
            DataType::Bytes => todo!(),
        };

        Ok(Point {
            id,
            name: conf.name,
            r#type: conf.r#type,
            slave: conf.slave,
            area: conf.area,
            address: conf.address - 1,
            quantity,
            value: Value::Null,
            describe: conf.describe,
        })
    }

    pub async fn update(&mut self, req: &CreatePointReq) -> Result<()> {
        let update_conf: UpdateConf = serde_json::from_value(req.clone())?;
        if let Some(name) = update_conf.name {
            self.name = name;
        }
        if let Some(r#type) = update_conf.r#type {
            self.r#type = r#type;
            // TODO 更改quantity
        }
        if let Some(slave) = update_conf.slave {
            self.slave = slave;
        }
        if let Some(area) = update_conf.area {
            self.area = area;
        }
        if let Some(address) = update_conf.address {
            self.address = address;
        }
        if let Some(describe) = update_conf.describe {
            self.describe = Some(describe);
        }

        Ok(())
    }

    pub fn set_data(&mut self, data: Vec<u8>) {
        self.value = get_value(&self.r#type, data);
    }
}

fn get_value(data_type: &DataType, data: Vec<u8>) -> Value {
    match data_type {
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
            if *endian == LittleEndian {
                data.swap(0, 1);
            }
            Value::from(i16::from_be_bytes(data))
        }
        DataType::Uint16(endian) => {
            let mut data = match data.as_slice() {
                [a, b] => [*a, *b],
                _ => return Value::Null,
            };
            if *endian == LittleEndian {
                data.swap(0, 1);
            }
            Value::from(i16::from_be_bytes(data))
        }
        DataType::Int32(endian0, endian1) => {
            let mut data = match data.as_slice() {
                [a, b, c, d] => [*a, *b, *c, *d],
                _ => return Value::Null,
            };
            if *endian0 == LittleEndian {
                data.swap(0, 1);
            }
            if *endian1 == LittleEndian {
                data.swap(2, 3);
            }
            Value::from(i32::from_be_bytes(data))
        }
        DataType::Uint32(endian0, endian1) => {
            let mut data = match data.as_slice() {
                [a, b, c, d] => [*a, *b, *c, *d],
                _ => return Value::Null,
            };
            if *endian0 == LittleEndian {
                data.swap(0, 1);
            }
            if *endian1 == LittleEndian {
                data.swap(2, 3);
            }
            Value::from(u32::from_be_bytes(data))
        }
        DataType::Int64(endian0, endian1, endian2, endian3) => {
            let mut data = match data.as_slice() {
                [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                _ => return Value::Null,
            };
            if *endian0 == LittleEndian {
                data.swap(0, 1);
            }
            if *endian1 == LittleEndian {
                data.swap(2, 3);
            }
            if *endian2 == LittleEndian {
                data.swap(4, 5);
            }
            if *endian3 == LittleEndian {
                data.swap(6, 7);
            }

            Value::from(i64::from_be_bytes(data))
        }
        DataType::Uint64(endian0, endian1, endian2, endian3) => {
            let mut data = match data.as_slice() {
                [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                _ => return Value::Null,
            };
            if *endian0 == LittleEndian {
                data.swap(0, 1);
            }
            if *endian1 == LittleEndian {
                data.swap(2, 3);
            }
            if *endian2 == LittleEndian {
                data.swap(4, 5);
            }
            if *endian3 == LittleEndian {
                data.swap(6, 7);
            }
            Value::from(u64::from_be_bytes(data))
        }
        DataType::Float32(endian0, endian1) => {
            let mut data = match data.as_slice() {
                [a, b, c, d] => [*a, *b, *c, *d],
                _ => return Value::Null,
            };
            if *endian0 == LittleEndian {
                data.swap(0, 1);
            }
            if *endian1 == LittleEndian {
                data.swap(2, 3);
            }
            Value::from(f32::from_be_bytes(data))
        }
        DataType::Float64(endian0, endian1, endian2, endian3) => {
            let mut data = match data.as_slice() {
                [a, b, c, d, e, f, g, h] => [*a, *b, *c, *d, *e, *f, *g, *h],
                _ => return Value::Null,
            };
            if *endian0 == LittleEndian {
                data.swap(0, 1);
            }
            if *endian1 == LittleEndian {
                data.swap(2, 3);
            }
            if *endian2 == LittleEndian {
                data.swap(4, 5);
            }
            if *endian3 == LittleEndian {
                data.swap(6, 7);
            }
            Value::from(f64::from_be_bytes(data))
        }
        // DataType::String => todo!(),
        // DataType::Bytes => todo!(),
        _ => Value::Null,
    }
}

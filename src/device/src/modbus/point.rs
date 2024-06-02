use anyhow::Result;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

use types::device::{
    CreatePointReq, DataType,
    Endian::{BigEndian, LittleEndian},
};

#[derive(Debug)]
pub(crate) struct Point {
    pub id: u64,
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
    pub fn new(conf: CreatePointReq, id: u64) -> Result<Point> {
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
            DataType::Float64(_, _, _, _) => 2,
            DataType::Bit => 1,
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

    pub async fn update(&mut self, update_point: Value) -> Result<()> {
        let update_conf: UpdateConf = serde_json::from_value(update_point)?;
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

    // modbus默认为大端
    pub fn set_data(&mut self, data: Vec<u8>) {
        self.value = get_value(&self.r#type, data);
        debug!("{:?}", self.value);
    }
}

fn get_value(data_type: &DataType, mut data: Vec<u8>) -> Value {
    match data_type {
        // DataType::Bool => {
        //     if buf.len() != 1 {
        //         warn!("buf is not right");
        //         Value::Null
        //     } else {
        //         info!("{:?}", buf);
        //         Value::Null
        //     }
        // }
        DataType::Int16(endian) => {
            if data.len() != 1 {
                warn!("buf is not right");
                Value::Null
            } else {
                match endian {
                    BigEndian => {
                        // BE::from_slice_u8(&mut data);
                        Value::from(data[0])
                    }
                    LittleEndian => Value::from(data[0]),
                }
            }
        }
        // DataType::Uint16(endian) => {
        //     if data.len() == 2 {
        //         match endian {
        //             BigEndian => {
        //                 let array: [u8; 2] = [data[0], data[1]];
        //                 Value::from(u16::from_be_bytes(array))
        //             }
        //             LittleEndian => {
        //                 let array: [u8; 2] = [data[1], data[0]];
        //                 Value::from(u16::from_be_bytes(array))
        //             }
        //         }
        //     } else {
        //         Value::Null
        //     }
        // }
        // DataType::Int32(endian0, endian1) => {
        //     if data.len() == 4 {
        //         match (endian0, endian1) {
        //             (BigEndian, BigEndian) => {
        //                 let array: [u8; 4] = [data[0], data[1], data[2], data[3]];
        //                 Value::from(i32::from_be_bytes(array))
        //             }
        //             (BigEndian, LittleEndian) => {
        //                 let array: [u8; 4] = [data[0], data[1], data[3], data[2]];
        //                 Value::from(i32::from_be_bytes(array))
        //             }
        //             (LittleEndian, BigEndian) => {
        //                 let array: [u8; 4] = [data[1], data[0], data[2], data[3]];
        //                 Value::from(i32::from_be_bytes(array))
        //             }
        //             (LittleEndian, LittleEndian) => {
        //                 let array: [u8; 4] = [data[1], data[0], data[3], data[2]];
        //                 Value::from(i32::from_be_bytes(array))
        //             }
        //         }
        //     } else {
        //         Value::Null
        //     }
        // }
        // DataType::Uint32(endian0, endian1) => {
        //     if data.len() == 4 {
        //         match (endian0, endian1) {
        //             (BigEndian, BigEndian) => {
        //                 let array: [u8; 4] = [data[0], data[1], data[2], data[3]];
        //                 Value::from(u32::from_be_bytes(array))
        //             }
        //             (BigEndian, LittleEndian) => {
        //                 let array: [u8; 4] = [data[0], data[1], data[3], data[2]];
        //                 Value::from(u32::from_be_bytes(array))
        //             }
        //             (LittleEndian, BigEndian) => {
        //                 let array: [u8; 4] = [data[1], data[0], data[2], data[3]];
        //                 Value::from(u32::from_be_bytes(array))
        //             }
        //             (LittleEndian, LittleEndian) => {
        //                 let array: [u8; 4] = [data[1], data[0], data[3], data[2]];
        //                 Value::from(u32::from_be_bytes(array))
        //             }
        //         }
        //     } else {
        //         Value::Null
        //     }
        // }
        // DataType::Int64(endian0, endian1, endian2, endian3) => {
        //     if data.len() == 8 {
        //         match (endian0, endian1, endian2, endian3) {
        //             (BigEndian, BigEndian, BigEndian, BigEndian) => {
        //                 let array: [u8; 8] = [
        //                     data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        //                 ];
        //                 Value::from(i64::from_be_bytes(array))
        //             }
        //             (BigEndian, BigEndian, BigEndian, LittleEndian) => {
        //                 let array: [u8; 8] = [
        //                     data[0], data[1], data[2], data[3], data[4], data[5], data[7], data[6],
        //                 ];
        //                 Value::from(i64::from_be_bytes(array))
        //             }
        //             (BigEndian, BigEndian, LittleEndian, BigEndian) => todo!(),
        //             (BigEndian, BigEndian, LittleEndian, LittleEndian) => todo!(),
        //             (BigEndian, LittleEndian, BigEndian, BigEndian) => todo!(),
        //             (BigEndian, LittleEndian, BigEndian, LittleEndian) => todo!(),
        //             (BigEndian, LittleEndian, LittleEndian, BigEndian) => todo!(),
        //             (BigEndian, LittleEndian, LittleEndian, LittleEndian) => todo!(),
        //             (LittleEndian, BigEndian, BigEndian, BigEndian) => todo!(),
        //             (LittleEndian, BigEndian, BigEndian, LittleEndian) => todo!(),
        //             (LittleEndian, BigEndian, LittleEndian, BigEndian) => todo!(),
        //             (LittleEndian, BigEndian, LittleEndian, LittleEndian) => todo!(),
        //             (LittleEndian, LittleEndian, BigEndian, BigEndian) => todo!(),
        //             (LittleEndian, LittleEndian, BigEndian, LittleEndian) => todo!(),
        //             (LittleEndian, LittleEndian, LittleEndian, BigEndian) => todo!(),
        //             (LittleEndian, LittleEndian, LittleEndian, LittleEndian) => todo!(),
        //         }
        //     } else {
        //         Value::Null
        //     }
        // }
        // DataType::Uint64(_, _, _, _) => todo!(),
        // DataType::Float32(_, _) => todo!(),
        // DataType::Float64(_, _, _, _) => todo!(),
        // DataType::Bit => todo!(),
        // DataType::String => todo!(),
        // DataType::Bytes => todo!(),
        _ => Value::Null,
    }
}

use common::error::Result;
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use types::device::{CreatePointReq, DataType, Endian::LittleEndian};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct Point {
    pub id: Uuid,
    pub conf: Conf,
    pub name: String,
    pub quantity: u16,
    pub value: Value,
}

#[derive(Deserialize, Debug)]
pub(crate) struct Conf {
    pub r#type: DataType,
    pub slave: u8,
    pub area: u8,
    pub address: u16,
    pub describe: Option<String>,
}

impl Point {
    pub fn new(req: CreatePointReq, id: Uuid) -> Result<Point> {
        let conf: Conf = serde_json::from_value(req.conf)?;
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
            conf,
            name: req.name,
            quantity,
            value: Value::Null,
        })
    }

    pub async fn update(&mut self, req: &CreatePointReq) -> Result<()> {
        let conf: Conf = serde_json::from_value(req.conf.clone())?;
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

        self.conf = conf;
        self.name = req.name.clone();
        self.quantity = quantity;

        Ok(())
    }

    // TODO
    pub async fn write(&mut self, req: &CreatePointReq) -> Result<()> {
        let conf: Conf = serde_json::from_value(req.conf.clone())?;
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

        self.conf = conf;
        self.name = req.name.clone();
        self.quantity = quantity;

        Ok(())
    }

    pub fn set_data(&mut self, data: Vec<u8>) {
        self.value = self.conf.r#type.decode(data);
    }
}

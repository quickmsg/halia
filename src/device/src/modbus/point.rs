use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use json::Value;
use protocol::modbus::{
    client::{Context, Reader, Writer},
    SlaveContext,
};
use serde::Deserialize;
use tracing::{debug, error, warn};
use types::device::{CreatePointReq, DataType};
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
    pub fn new(req: CreatePointReq, id: Uuid) -> HaliaResult<Point> {
        let conf: Conf = serde_json::from_value(req.conf)?;
        let quantity = conf.r#type.get_quantity();
        Ok(Point {
            id,
            conf,
            name: req.name,
            quantity,
            value: Value::Null,
        })
    }

    pub async fn update(&mut self, req: &CreatePointReq) -> HaliaResult<()> {
        let conf: Conf = serde_json::from_value(req.conf.clone())?;
        self.quantity = conf.r#type.get_quantity();
        self.conf = conf;
        self.name = req.name.clone();
        Ok(())
    }

    pub async fn read(&mut self, ctx: &mut Context) -> HaliaResult<json::Value> {
        ctx.set_slave(self.conf.slave);
        match self.conf.area {
            0 => match ctx
                .read_discrete_inputs(self.conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        debug!("{:?}", data);
                        Ok(Value::Null)
                    }
                    Err(e) => {
                        warn!("modbus protocl exception:{}", e);
                        Ok(Value::Null)
                    }
                },
                Err(_) => todo!(),
            },
            1 => match ctx.read_coils(self.conf.address, self.quantity).await {
                Ok(_) => todo!(),
                Err(_) => todo!(),
            },
            4 => match ctx
                .read_input_registers(self.conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.r#type.decode(&mut data);
                        self.value = value.clone();
                        Ok(value)
                    }
                    Err(_) => todo!(),
                },
                Err(_) => todo!(),
            },
            3 => match ctx
                .read_holding_registers(self.conf.address, self.quantity)
                .await
            {
                Ok(_) => todo!(),
                Err(_) => todo!(),
            },
            _ => unreachable!(),
        }
    }

    pub async fn write(&mut self, ctx: &mut Context, value: serde_json::Value) -> Result<()> {
        ctx.set_slave(self.conf.slave);
        match self.conf.area {
            // TODO
            0 => match ctx.write_single_coil(self.conf.address, 1).await {
                Ok(res) => match res {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        warn!("modbus protocl exception:{}", e);
                        todo!()
                    }
                },
                Err(_) => todo!(),
            },
            4 => match self.conf.r#type {
                // DataType::Bool => todo!(),
                DataType::Int16(_) | DataType::Uint16(_) => {
                    let data = self.conf.r#type.encode(value).unwrap();
                    match ctx.write_single_register(self.conf.address, &data).await {
                        Ok(_) => return Ok(()),
                        Err(e) => bail!("{}", e),
                    }
                }
                DataType::Int32(_, _)
                | DataType::Uint32(_, _)
                | DataType::Int64(_, _)
                | DataType::Uint64(_, _)
                | DataType::Float32(_, _)
                | DataType::Float64(_, _) => {
                    let data = self.conf.r#type.encode(value).unwrap();
                    debug!("data is {:?}", data);
                    match ctx.write_multiple_registers(self.conf.address, &data).await {
                        Ok(_) => return Ok(()),
                        Err(e) => bail!("{}", e),
                    }
                }
                // DataType::String => todo!(),
                // DataType::Bytes => todo!(),
                _ => bail!("not support"),
            },
            _ => unreachable!(),
        }
    }
}

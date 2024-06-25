use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use message::MessageValue;
use protocol::modbus::{
    client::{Context, Reader, Writer},
    SlaveContext,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, error, warn};
use types::device::{
    datatype::{DataType, Endian},
    point::CreatePointReq,
};
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct Point {
    pub id: Uuid,
    pub conf: Conf,
    pub name: String,
    pub quantity: u16,
    pub value: Value,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
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

    pub async fn read(&mut self, ctx: &mut Context) -> HaliaResult<MessageValue> {
        ctx.set_slave(self.conf.slave);
        match self.conf.area {
            0 => match ctx
                .read_discrete_inputs(self.conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.r#type.decode(&mut data);
                        self.value = value.clone().into();
                        Ok(value)
                    }
                    Err(e) => {
                        warn!("modbus protocl exception:{}", e);
                        Ok(MessageValue::Null)
                    }
                },
                Err(_) => todo!(),
            },
            1 => match ctx.read_coils(self.conf.address, self.quantity).await {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.r#type.decode(&mut data);
                        self.value = value.clone().into();
                        Ok(value)
                    }
                    Err(e) => {
                        warn!("modbus protocl exception:{}", e);
                        Ok(MessageValue::Null)
                    }
                },
                Err(_) => todo!(),
            },
            4 => match ctx
                .read_input_registers(self.conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.r#type.decode(&mut data);
                        self.value = value.clone().into();
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
        Ok(match self.conf.area {
            0 => {
                if let Ok(data) = self.conf.r#type.encode(value) {
                    match data.len() {
                        1 => match ctx.write_single_coil(self.conf.address, data[0]).await {
                            Ok(res) => match res {
                                Ok(_) => return Ok(()),
                                Err(e) => {
                                    warn!("modbus protocl exception:{}", e);
                                    return Ok(());
                                }
                            },
                            Err(e) => return Err(e.into()),
                        },
                        _ => match ctx.write_multiple_coils(self.conf.address, &data).await {
                            Ok(res) => match res {
                                Ok(_) => return Ok(()),
                                Err(e) => {
                                    warn!("modbus protocl exception:{}", e);
                                    return Ok(());
                                }
                            },
                            Err(e) => return Err(e.into()),
                        },
                    }
                }
            }
            4 => {
                if let Ok(data) = self.conf.r#type.encode(value) {
                    match data.len() {
                        1 => match self.conf.r#type {
                            DataType::Int8(endian) | DataType::Uint8(endian) => {
                                let (and_mask, or_mask) = match endian {
                                    Endian::BigEndian => (0x00FF, (data[0] as u16) << 8),
                                    Endian::LittleEndian => (0xFF00, data[0] as u16),
                                };
                                match ctx
                                    .masked_write_register(self.conf.address, and_mask, or_mask)
                                    .await
                                {
                                    Ok(res) => match res {
                                        Ok(_) => return Ok(()),
                                        Err(e) => {
                                            warn!("modbus protocol exception:{}", e);
                                            return Ok(());
                                        }
                                    },
                                    Err(e) => bail!("{}", e),
                                }
                            }
                            DataType::Bool(pos) => {
                                todo!()
                            }
                            _ => todo!(),
                        },
                        2 => match ctx.write_single_register(self.conf.address, &data).await {
                            Ok(res) => match res {
                                Ok(_) => return Ok(()),
                                Err(e) => {
                                    warn!("modbus protocol exception:{}", e);
                                    return Ok(());
                                }
                            },
                            Err(e) => bail!("{}", e),
                        },
                        _ => match ctx.write_single_register(self.conf.address, &data).await {
                            Ok(res) => match res {
                                Ok(_) => return Ok(()),
                                Err(e) => {
                                    warn!("modbus protocol exception:{}", e);
                                    return Ok(());
                                }
                            },
                            Err(e) => bail!("{}", e),
                        },
                    }
                }
            }
            _ => unreachable!(),
        })
    }
}

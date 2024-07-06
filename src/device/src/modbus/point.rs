use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use message::MessageValue;
use protocol::modbus::{
    client::{Context, Reader, Writer},
    SlaveContext,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, warn};
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

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Area {
    InputDiscrete,
    Coils, // 可读写
    InputRegisters,
    HoldingRegisters, // 可读写
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Conf {
    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub desc: Option<String>,
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
            Area::InputDiscrete => match ctx
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
                Err(_) => Err(HaliaError::Disconnect),
            },
            Area::Coils => match ctx.read_coils(self.conf.address, self.quantity).await {
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
                Err(_) => Err(HaliaError::Disconnect),
            },
            Area::InputRegisters => match ctx
                .read_input_registers(self.conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.r#type.decode(&mut data);
                        self.value = value.clone().into();
                        Ok(value)
                    }
                    Err(_) => Ok(MessageValue::Null),
                },
                Err(_) => Err(HaliaError::Disconnect),
            },
            Area::HoldingRegisters => match ctx
                .read_holding_registers(self.conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.r#type.decode(&mut data);
                        self.value = value.clone().into();
                        Ok(value)
                    }
                    Err(_) => Ok(MessageValue::Null),
                },
                Err(_) => Err(HaliaError::Disconnect),
            },
        }
    }
}

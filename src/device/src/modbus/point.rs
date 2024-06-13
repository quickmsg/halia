use common::error::{HaliaError, HaliaResult};
use json::Value;
use protocol::modbus::{client::{Context, Reader}, SlaveContext};
use serde::Deserialize;
use tracing::warn;
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
                    Ok(mut data) => todo!(),
                    Err(e) => {
                        warn!("modbus protocl exception:{}", e);
                        todo!()
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
}

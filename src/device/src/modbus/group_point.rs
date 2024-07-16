use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageValue;
use protocol::modbus::{
    client::{Context, Reader},
    SlaveContext,
};
use serde_json::Value;
use tracing::warn;
use types::devices::modbus::{Area, CreateUpdateGroupPointReq};
use uuid::Uuid;

#[derive(Debug)]
pub struct Point {
    pub id: Uuid,
    pub conf: CreateUpdateGroupPointReq,
    pub quantity: u16,
    pub value: Value,
}

impl Point {
    pub async fn new(
        device_id: &Uuid,
        group_id: &Uuid,
        point_id: Option<Uuid>,
        req: CreateUpdateGroupPointReq,
    ) -> HaliaResult<Point> {
        let (point_id, new) = match point_id {
            Some(point_id) => (point_id, false),
            None => (Uuid::new_v4(), true),
        };

        let quantity = req.r#type.get_quantity();

        if new {
            persistence::modbus::create_group_point(
                device_id,
                group_id,
                &point_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Point {
            id: point_id,
            conf: req,
            quantity,
            value: Value::Null,
        })
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        group_id: &Uuid,
        req: CreateUpdateGroupPointReq,
    ) -> HaliaResult<()> {
        persistence::modbus::update_group_point(
            device_id,
            group_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        self.quantity = req.r#type.get_quantity();
        self.conf = req;

        Ok(())
    }

    pub async fn delete(&self, device_id: &Uuid, group_id: &Uuid) -> HaliaResult<()> {
        persistence::modbus::delete_group_point(device_id, group_id, &self.id).await?;
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

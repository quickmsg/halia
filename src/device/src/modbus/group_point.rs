use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageValue;
use protocol::modbus::{
    client::{Context, Reader},
    SlaveContext,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;
use types::device::datatype::DataType;
use uuid::Uuid;

#[derive(Debug)]
pub struct Point {
    pub id: Uuid,
    pub conf: Conf,
    pub quantity: u16,
    pub value: Value,
}

#[derive(Deserialize, Debug, Clone, Serialize)]
pub struct Conf {
    pub name: String,
    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Area {
    InputDiscrete,
    Coils, // 可读写
    InputRegisters,
    HoldingRegisters, // 可读写
}

impl Point {
    pub async fn new(
        device_id: &Uuid,
        group_id: &Uuid,
        point_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<Point> {
        let (point_id, new) = match point_id {
            Some(point_id) => (point_id, false),
            None => (Uuid::new_v4(), true),
        };

        let conf: Conf = serde_json::from_str(&data)?;
        let quantity = conf.r#type.get_quantity();

        if new {
            persistence::modbus::create_group_point(device_id, group_id, &point_id, &data).await?;
        }

        Ok(Point {
            id: point_id,
            conf,
            quantity,
            value: Value::Null,
        })
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        group_id: &Uuid,
        data: String,
    ) -> HaliaResult<()> {
        let conf: Conf = serde_json::from_str(&data)?;

        persistence::modbus::update_group_point(device_id, group_id, &self.id, &data).await?;

        self.quantity = conf.r#type.get_quantity();
        self.conf = conf;

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

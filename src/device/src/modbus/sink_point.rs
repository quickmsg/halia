use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use types::devices::datatype::DataType;
use uuid::Uuid;

use super::group_point::Area;

#[derive(Debug, Clone)]
pub struct Point {
    pub id: Uuid,
    pub conf: Conf,
    pub value: TargetValue,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Conf {
    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    pub value: serde_json::Value,
    pub desc: Option<String>,
}

#[derive(Serialize)]
struct SearchResp {
    id: Uuid,
    conf: Conf,
}

#[derive(Debug, Clone)]
pub enum TargetValue {
    Int(i64),
    Uint(u64),
    Float(f64),
    Boolean(bool),
    String(String),
    Null,
    Field(String),
}

impl Point {
    pub async fn new(
        device_id: &Uuid,
        sink_id: &Uuid,
        point_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<Self> {
        let conf: Conf = serde_json::from_str(&data)?;

        let (point_id, new) = match point_id {
            Some(point_id) => (point_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::modbus::create_sink_point(device_id, sink_id, &point_id, &data).await?;
        }

        let value = match &conf.value {
            serde_json::Value::Null => TargetValue::Null,
            serde_json::Value::Bool(b) => TargetValue::Boolean(*b),
            serde_json::Value::Number(n) => {
                if let Some(uint) = n.as_u64() {
                    TargetValue::Uint(uint)
                } else if let Some(int) = n.as_i64() {
                    TargetValue::Int(int)
                } else if let Some(float) = n.as_f64() {
                    TargetValue::Float(float)
                } else {
                    unreachable!()
                }
            }
            serde_json::Value::String(s) => {
                if s.starts_with("'") && s.ends_with("'") && s.len() >= 3 {
                    TargetValue::String(s.trim_start_matches("'").trim_end_matches("'").to_string())
                } else {
                    TargetValue::Field(s.clone())
                }
            }
            _ => return Err(HaliaError::ConfErr),
        };

        Ok(Point {
            id: point_id,
            conf,
            value,
        })
    }

    pub fn search(&self) -> serde_json::Value {
        json!(SearchResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        })
    }

    pub async fn update(&mut self, data: String) -> HaliaResult<bool> {
        let update_conf: Conf = serde_json::from_str(&data)?;

        let mut restart = false;

        if self.conf.r#type != update_conf.r#type
            || self.conf.slave != update_conf.slave
            || self.conf.area != update_conf.area
            || self.conf.address != update_conf.address
            || self.conf.value != update_conf.value
        {
            restart = true;
        }

        self.conf = update_conf;

        Ok(restart)
    }

    pub async fn delete(&self, device_id: &Uuid, sink_id: &Uuid) -> HaliaResult<()> {
        persistence::modbus::delete_sink_point(device_id, sink_id, &self.id).await?;
        Ok(())
    }
}

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use types::devices::modbus::{CreateUpdateSinkPointReq, SearchSinkPointsItemResp};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Point {
    pub id: Uuid,
    pub conf: CreateUpdateSinkPointReq,
    pub value: TargetValue,
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
        req: CreateUpdateSinkPointReq,
    ) -> HaliaResult<Self> {
        let (point_id, new) = match point_id {
            Some(point_id) => (point_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::modbus::create_sink_point(
                device_id,
                sink_id,
                &point_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        let value = match &req.value {
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
            conf: req,
            value,
        })
    }

    pub fn search(&self) -> SearchSinkPointsItemResp {
        SearchSinkPointsItemResp {
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, req: CreateUpdateSinkPointReq) -> HaliaResult<bool> {
        let mut restart = false;
        if self.conf.r#type != req.r#type
            || self.conf.slave != req.slave
            || self.conf.area != req.area
            || self.conf.address != req.address
            || self.conf.value != req.value
        {
            restart = true;
        }

        self.conf = req;

        Ok(restart)
    }

    pub async fn delete(&self, device_id: &Uuid, sink_id: &Uuid) -> HaliaResult<()> {
        persistence::modbus::delete_sink_point(device_id, sink_id, &self.id).await?;
        Ok(())
    }
}

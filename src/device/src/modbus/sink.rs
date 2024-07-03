use common::error::{HaliaError, HaliaResult};
use serde::Deserialize;
use types::device::datatype::DataType;
use uuid::Uuid;

use super::point::Area;

struct Sink {
    id: Uuid,
    name: String,
    points: Vec<Point>,
}

impl Sink {
    fn new(id: Uuid, conf: serde_json::Value) -> HaliaResult<Self> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let mut points = vec![];
        for point_conf in conf.points {
            let point = Point::new(point_conf)?;
            points.push(point);
        }
        Ok(Sink {
            id,
            name: conf.name,
            points,
        })
    }
}

#[derive(Deserialize)]
struct SinkConf {
    name: String,
    points: Vec<PointConf>,
}

struct Point {
    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    value: TargetValue,
}

#[derive(Deserialize)]
struct PointConf {
    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    value: serde_json::Value,
}

enum TargetValue {
    Int(i64),
    Uint(u64),
    Float(f64),
    Boolean(bool),
    String(String),
    Null,
    Field(String),
}

impl Point {
    fn new(conf: PointConf) -> HaliaResult<Self> {
        let value = match conf.value {
            serde_json::Value::Null => TargetValue::Null,
            serde_json::Value::Bool(b) => TargetValue::Boolean(b),
            serde_json::Value::Number(n) => {
                if let Some(uint) = n.as_u64() {
                    TargetValue::Uint(uint)
                } else if let Some(int) = n.as_i64() {
                    TargetValue::Int(int)
                } else if let Some(float) = n.as_f64() {
                    TargetValue::Float(float)
                } else {
                    // TODO
                    return Err(HaliaError::NotFound);
                }
            }
            serde_json::Value::String(s) => TargetValue::String(s),
            // serde_json::Value::Array(_) => todo!(),
            // serde_json::Value::Object(_) => todo!(),
            _ => return Err(HaliaError::ConfErr),
        };

        Ok(Point {
            r#type: conf.r#type,
            slave: conf.slave,
            area: conf.area,
            address: conf.address,
            value,
        })
    }
}

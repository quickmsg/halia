use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::debug;
use types::device::datatype::DataType;
use uuid::Uuid;

use super::point::Area;

#[derive(Debug)]
pub struct Sink {
    pub id: Uuid,
    points: Vec<Point>,
    conf: SinkConf,
    pub tx: Option<mpsc::Sender<MessageBatch>>,
}

pub struct Command {}

pub fn new(id: Uuid, conf: SinkConf) -> HaliaResult<Sink> {
    let mut points = vec![];
    for point_conf in &conf.points {
        let point = Point::new(point_conf)?;
        points.push(point);
    }
    Ok(Sink {
        id,
        points,
        conf,
        tx: None,
    })
}

impl Sink {
    pub fn run(&self, mut rx: mpsc::Receiver<MessageBatch>) {
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(mb) => {
                        debug!("{mb:?}");
                        //
                    }
                    None => return,
                }
            }
        });
    }

    pub fn search(&self) -> serde_json::Value {
        let resp = SinkSearchResp {
            id: self.id.clone(),
            name: self.conf.name.clone(),
            ref_cnt: 0,
            points: self.conf.points.clone(),
        };
        serde_json::to_value(resp).unwrap()
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct SinkConf {
    name: String,
    points: Vec<PointConf>,
}

#[derive(Serialize)]
struct SinkSearchResp {
    id: Uuid,
    name: String,
    ref_cnt: usize,
    points: Vec<PointConf>,
}

#[derive(Debug)]
struct Point {
    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    value: TargetValue,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct PointConf {
    pub r#type: DataType,
    pub slave: u8,
    pub area: Area,
    pub address: u16,
    value: serde_json::Value,
}

#[derive(Debug)]
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
    fn new(conf: &PointConf) -> HaliaResult<Self> {
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
                    // TODO
                    return Err(HaliaError::NotFound);
                }
            }
            serde_json::Value::String(s) => TargetValue::String(s.clone()),
            // serde_json::Value::Array(_) => todo!(),
            // serde_json::Value::Object(_) => todo!(),
            _ => return Err(HaliaError::ConfErr),
        };

        Ok(Point {
            r#type: conf.r#type.clone(),
            slave: conf.slave,
            area: conf.area.clone(),
            address: conf.address,
            value,
        })
    }
}

use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::debug;
use types::device::{datatype::DataType, device::SearchSinksResp};
use uuid::Uuid;

use super::{point::Area, WritePointEvent};

#[derive(Debug)]
pub struct SinkManager {
    sinks: Arc<RwLock<Vec<Sink>>>,
}

impl SinkManager {
    pub fn new() -> Self {
        SinkManager {
            sinks: Arc::new(RwLock::new(vec![])),
        }
    }

    pub async fn add_sink(&self, id: Uuid, conf: SinkConf) -> HaliaResult<()> {
        let mut points = vec![];
        for point_conf in &conf.points {
            let point = Point::new(point_conf)?;
            points.push(point);
        }
        self.sinks.write().await.push(Sink {
            id,
            points: Arc::new(RwLock::new(points)),
            conf,
        });

        Ok(())
    }

    pub async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp {
        let mut data = vec![];
        let mut i = 0;
        for sink in self.sinks.read().await.iter().rev().skip((page - 1) * size) {
            data.push(sink.search());
            i += 1;
            if i >= size {
                break;
            }
        }
        SearchSinksResp {
            total: self.sinks.read().await.len(),
            data,
        }
    }

    pub async fn update_sink(&self, sink_id: Uuid, conf: SinkConf) -> HaliaResult<()> {
        match self
            .sinks
            .write()
            .await
            .iter_mut()
            .find(|sink| sink.id == sink_id)
        {
            Some(sink) => {
                let mut points = vec![];
                for point_conf in &conf.points {
                    let point = Point::new(point_conf)?;
                    points.push(point);
                }
                *sink.points.write().await = points;
                sink.conf = conf;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_sink(&self, sink_id: Uuid) -> HaliaResult<()> {
        self.sinks.write().await.retain(|sink| sink.id != sink_id);
        Ok(())
    }

    pub fn run(
        &self,
        // mut command_rx: mpsc::Receiver<Command>,
        mut rx: mpsc::Receiver<MessageBatch>,
        tx: mpsc::Sender<WritePointEvent>,
    ) {
        let sinks = self.sinks.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(mb) => {
                        if let Some(sink_id) = mb.get_sink_id() {
                            match sinks.read().await.iter().find(|sink| sink.id == *sink_id) {
                                Some(sink) => {
                                    for point in sink.points.read().await.iter() {
                                        let value = match &point.value {
                                            TargetValue::Int(n) => serde_json::json!(n),
                                            TargetValue::Uint(un) => serde_json::json!(un),
                                            TargetValue::Float(f) => serde_json::json!(f),
                                            TargetValue::Boolean(b) => serde_json::json!(b),
                                            TargetValue::String(s) => serde_json::json!(s),
                                            TargetValue::Null => serde_json::Value::Null,
                                            TargetValue::Field(field) => {
                                                let messages = mb.get_messages();
                                                if messages.len() > 0 {
                                                    match messages[0].get(field) {
                                                        Some(value) => match value {
                                                            message::MessageValue::Null => {
                                                                serde_json::Value::Null
                                                            }
                                                            message::MessageValue::Boolean(b) => {
                                                                serde_json::json!(b)
                                                            }
                                                            message::MessageValue::Int64(n) => {
                                                                serde_json::json!(n)
                                                            }
                                                            message::MessageValue::Uint64(ui) => {
                                                                serde_json::json!(ui)
                                                            }
                                                            message::MessageValue::Float64(f) => {
                                                                serde_json::json!(f)
                                                            }
                                                            message::MessageValue::String(s) => {
                                                                serde_json::json!(s)
                                                            }
                                                            message::MessageValue::Bytes(_) => {
                                                                todo!()
                                                            }
                                                            _ => continue,
                                                        },
                                                        None => continue,
                                                    }
                                                } else {
                                                    continue;
                                                }
                                            }
                                        };

                                        match WritePointEvent::new(
                                            point.slave,
                                            point.area.clone(),
                                            point.address,
                                            point.r#type.clone(),
                                            value,
                                        ) {
                                            Ok(wpe) => {
                                                let _ = tx.send(wpe).await;
                                            }
                                            Err(e) => {
                                                debug!("value is err");
                                            }
                                        }
                                    }
                                }
                                None => {}
                            }
                        }
                    }
                    None => return,
                }
            }
        });
    }
}

#[derive(Debug)]
pub struct Sink {
    pub id: Uuid,
    points: Arc<RwLock<Vec<Point>>>,
    conf: SinkConf,
}

pub enum Command {
    Start,
    Stop,
}

impl Sink {
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
            r#type: conf.r#type.clone(),
            slave: conf.slave,
            area: conf.area.clone(),
            address: conf.address,
            value,
        })
    }
}

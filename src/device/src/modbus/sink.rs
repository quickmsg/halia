use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::{select, sync::mpsc};
use tracing::{debug, warn};
use types::device::{datatype::DataType, device::SearchSinksResp};
use uuid::Uuid;

use super::{point::Area, WritePointEvent};

#[derive(Debug)]
pub struct Sink {
    pub id: Uuid,
    pub name: String,
    desc: Option<String>,

    ref_cnt: AtomicUsize,
    pub on: AtomicBool,
    points: Vec<Point>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    publish_tx: Option<mpsc::Sender<MessageBatch>>,
}

impl Sink {
    pub fn new(sink_id: Uuid, data: &String) -> HaliaResult<Self> {
        let conf: SinkConf = serde_json::from_str(&data)?;
        Ok(Sink {
            id: sink_id,
            name: conf.name.clone(),
            on: AtomicBool::new(false),
            ref_cnt: AtomicUsize::new(0),
            desc: conf.desc.clone(),
            points: vec![],
            stop_signal_tx: None,
            publish_tx: None,
        })
    }

    pub fn publish(&mut self) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match &self.publish_tx {
            Some(tx) => Ok(tx.clone()),
            None => panic!("must start by mod"),
        }
    }

    pub async fn get_info(&self) -> serde_json::Value {
        let resp = SinkSearchResp {
            id: self.id.clone(),
            name: self.name.clone(),
            desc: self.desc.clone(),
            on: self.on.load(Ordering::SeqCst),
            ref_cnt: self.ref_cnt.load(Ordering::SeqCst),
            point_cnt: self.points.len(),
        };
        json!(resp)
    }

    pub async fn create_point(&mut self, point_id: Uuid, data: &String) -> HaliaResult<()> {
        let point = Point::new(data)?;
        self.points.push(point);
        Ok(())
    }

    // pub fn search(&self) -> serde_json::Value {
    //     let resp = SinkSearchResp {
    //         id: self.id.clone(),
    //         name: self.conf.name.clone(),
    //         ref_cnt: 0,
    //     };
    //     serde_json::to_value(resp).unwrap()
    // }

    pub fn start(&mut self, tx: mpsc::Sender<WritePointEvent>) {
        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (publish_tx, mut publish_rx) = mpsc::channel(16);
        self.publish_tx = Some(publish_tx);

        let points = self.points.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        debug!("sink stop");
                        return
                    }

                    mb = publish_rx.recv() => {
                        if let Some(mb) = mb {
                            for point in points.iter() {
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
                                        let _ = tx.send(wpe);
                                    }
                                    Err(e) => {
                                        debug!("value is err");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn stop(&mut self) {
        match &self.stop_signal_tx {
            Some(tx) => {
                if let Err(e) = tx.send(()).await {
                    warn!("stop signal send err :{e}");
                }
                self.stop_signal_tx = None;
            }
            None => {}
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct SinkConf {
    name: String,
    desc: Option<String>,
}

#[derive(Serialize)]
struct SinkSearchResp {
    id: Uuid,
    name: String,
    desc: Option<String>,
    on: bool,
    ref_cnt: usize,
    point_cnt: usize,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
    fn new(data: &String) -> HaliaResult<Self> {
        let conf: PointConf = serde_json::from_str(data)?;
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

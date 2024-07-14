use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{select, sync::mpsc};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::modbus::sink_point::TargetValue;

use super::{sink_point::Point, WritePointEvent};

#[derive(Debug)]
pub struct Sink {
    pub id: Uuid,
    conf: Conf,

    ref_cnt: AtomicUsize,
    points: Vec<Point>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    publish_tx: Option<mpsc::Sender<MessageBatch>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Conf {
    name: String,
    desc: Option<String>,
}

#[derive(Serialize)]
struct SearchResp {
    id: Uuid,
    conf: Conf,
    point_cnt: usize,
}

impl Sink {
    pub async fn new(device_id: &Uuid, sink_id: Option<Uuid>, data: String) -> HaliaResult<Self> {
        let conf: Conf = serde_json::from_str(&data)?;

        let (sink_id, new) = match sink_id {
            Some(sink_id) => (sink_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::modbus::create_sink(device_id, &sink_id, &data).await?;
        }

        Ok(Sink {
            id: sink_id,
            conf,
            ref_cnt: AtomicUsize::new(0),
            points: vec![],
            stop_signal_tx: None,
            publish_tx: None,
        })
    }

    pub fn search(&self) -> serde_json::Value {
        let resp = SearchResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            point_cnt: self.points.len(),
        };
        json!(resp)
    }

    pub async fn update(&mut self, device_id: &Uuid, data: String) -> HaliaResult<()> {
        let update_conf: Conf = serde_json::from_str(&data)?;
        persistence::modbus::update_sink(device_id, &self.id, &data).await?;
        self.conf = update_conf;
        Ok(())
    }

    pub async fn delete(&mut self) {

    }

    pub fn publish(&mut self) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match &self.publish_tx {
            Some(tx) => Ok(tx.clone()),
            None => panic!("must start by mod"),
        }
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

#[derive(Serialize)]
struct SinkSearchResp {
    id: Uuid,
    name: String,
    desc: Option<String>,
    ref_cnt: usize,
    point_cnt: usize,
}

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use tokio::{select, sync::mpsc};
use tracing::{debug, warn};
use types::devices::modbus::{CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

use super::WritePointEvent;

#[derive(Debug)]
pub struct Sink {
    pub id: Uuid,
    conf: CreateUpdateSinkReq,
    ref_cnt: usize,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,
    publish_tx: Option<mpsc::Sender<MessageBatch>>,
    target_value: TargetValue,
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

impl Sink {
    pub async fn new(
        device_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<Self> {
        let (sink_id, new) = match sink_id {
            Some(sink_id) => (sink_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::modbus::create_sink(
                device_id,
                &sink_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        let target_value = match &req.value {
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

        Ok(Sink {
            id: sink_id,
            conf: req,
            ref_cnt: 0,
            stop_signal_tx: None,
            publish_tx: None,
            target_value,
        })
    }

    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        persistence::devices::modbus::update_sink(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        // let mut restart = false;
        // if self.conf.r#type != req.r#type
        //     || self.conf.slave != req.slave
        //     || self.conf.area != req.area
        //     || self.conf.address != req.address
        //     || self.conf.value != req.value
        // {
        //     restart = true;
        // }

        // self.conf = req;

        // Ok(restart)
        self.conf = req;

        // TODO restart
        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::modbus::delete_sink(device_id, &self.id).await?;
        match self.stop_signal_tx {
            Some(_) => self.stop().await,
            None => {}
        }

        Ok(())
    }

    pub fn publish(&mut self) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        self.ref_cnt += 1;
        match &self.publish_tx {
            Some(tx) => Ok(tx.clone()),
            None => panic!("must start by device"),
        }
    }

    pub async fn unpublish(&mut self) {
        self.ref_cnt -= 1;
        if self.ref_cnt == 0 {
            self.stop().await;
        }
    }

    pub fn start(&mut self, tx: mpsc::Sender<WritePointEvent>) {
        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (publish_tx, mut publish_rx) = mpsc::channel(16);
        self.publish_tx = Some(publish_tx);

        let conf = self.conf.clone();
        let value = self.target_value.clone();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        debug!("sink stop");
                        return
                    }

                    mb = publish_rx.recv() => {
                        if let Some(mb) = mb {
                            Sink::send_write_point_event(mb, &conf, &value, &tx);
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

    fn send_write_point_event(
        mb: MessageBatch,
        conf: &CreateUpdateSinkReq,
        value: &TargetValue,
        tx: &mpsc::Sender<WritePointEvent>,
    ) {
        let value = match value {
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
                            message::MessageValue::Null => serde_json::Value::Null,
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
                            _ => unreachable!(),
                        },
                        None => unreachable!(),
                    }
                } else {
                    unreachable!();
                }
            }
        };

        match WritePointEvent::new(
            conf.slave,
            conf.area.clone(),
            conf.address,
            conf.r#type.clone(),
            value,
        ) {
            Ok(wpe) => {
                let _ = tx.send(wpe);
            }
            Err(e) => {
                debug!("value is err :{e}");
            }
        }
    }
}

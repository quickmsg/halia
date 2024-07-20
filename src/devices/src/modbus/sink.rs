use common::{error::HaliaResult, persistence};
use message::MessageBatch;
use tokio::{select, sync::mpsc};
use tracing::{debug, warn};
use types::devices::modbus::{Area, CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

use super::WritePointEvent;

#[derive(Debug)]
pub struct Sink {
    pub id: Uuid,
    conf: CreateUpdateSinkReq,
    ref_cnt: usize,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,
    publish_tx: Option<mpsc::Sender<MessageBatch>>,
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

        Ok(Sink {
            id: sink_id,
            conf: req,
            ref_cnt: 0,
            stop_signal_tx: None,
            publish_tx: None,
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
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        debug!("sink stop");
                        return
                    }

                    mb = publish_rx.recv() => {
                        if let Some(mb) = mb {
                            Sink::send_write_point_event(mb, &conf, &tx);
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
        mut mb: MessageBatch,
        conf: &CreateUpdateSinkReq,
        tx: &mpsc::Sender<WritePointEvent>,
    ) {
        let message = match mb.take_one_message() {
            Some(message) => message,
            None => return,
        };

        let slave = match conf.slave.typ {
            types::devices::SinkValueType::Const => {
                match common::json::number::get_u8(&conf.slave.value) {
                    Some(n) => n,
                    None => {
                        warn!("slave只能是number类型");
                        return;
                    }
                }
            }
            types::devices::SinkValueType::Variable => match &conf.slave.value {
                serde_json::Value::String(field) => match message.get_u8(field) {
                    Some(n) => n,
                    None => return,
                },
                _ => {
                    warn!("字段名称错误");
                    return;
                }
            },
        };

        let area = match conf.area.typ {
            types::devices::SinkValueType::Const => {
                match common::json::number::get_u8(&conf.area.value) {
                    Some(n) => match Area::try_from(n) {
                        Ok(area) => area,
                        Err(_) => return,
                    },
                    None => {
                        warn!("area只能是number类型");
                        return;
                    }
                }
            }
            types::devices::SinkValueType::Variable => match &conf.area.value {
                serde_json::Value::String(field) => match message.get_u8(field) {
                    Some(n) => match Area::try_from(n) {
                        Ok(area) => area,
                        Err(_) => return,
                    },
                    None => return,
                },
                _ => return,
            },
        };

        let address = match conf.address.typ {
            types::devices::SinkValueType::Const => {
                match common::json::number::get_u16(&conf.address.value) {
                    Some(n) => n,
                    None => return,
                }
            }
            types::devices::SinkValueType::Variable => match &conf.address.value {
                serde_json::Value::String(field) => match message.get_u16(&field) {
                    Some(address) => address,
                    None => return,
                },
                _ => return,
            },
        };

        let value = match conf.value.typ {
            types::devices::SinkValueType::Const => conf.value.value.clone(),
            types::devices::SinkValueType::Variable => match &conf.address.value {
                serde_json::Value::String(field) => match message.get(field) {
                    Some(v) => v.clone().into(),
                    None => return,
                },
                _ => return,
            },
        };

        match WritePointEvent::new(slave, area, address, conf.r#type.clone(), value) {
            Ok(wpe) => {
                let _ = tx.send(wpe);
            }
            Err(e) => {
                debug!("value is err :{e}");
            }
        }
    }
}

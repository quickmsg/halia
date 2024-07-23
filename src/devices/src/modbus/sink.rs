use common::{error::HaliaResult, persistence};
use message::MessageBatch;
use tokio::{select, sync::mpsc, task::JoinHandle};
use tracing::{debug, warn};
use types::{
    devices::modbus::{
        Area, CreateUpdateSinkReq, DataType, Endian, SearchSinksItemResp, SinkConf, Type,
    },
    SinkValueType,
};
use uuid::Uuid;

use super::WritePointEvent;

#[derive(Debug)]
pub struct Sink {
    pub id: Uuid,
    conf: CreateUpdateSinkReq,

    pub on: bool,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
            mpsc::Sender<WritePointEvent>,
        )>,
    >,

    ref_rules: Vec<Uuid>,
    active_ref_rules: Vec<Uuid>,
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
            on: false,
            conf: req,
            stop_signal_tx: None,
            handle: None,
            ref_rules: vec![],
            active_ref_rules: vec![],
            publish_tx: None,
        })
    }

    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            ref_rules: self.ref_rules.clone(),
            active_ref_rules: self.active_ref_rules.clone(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        persistence::devices::modbus::update_sink(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.sink != req.sink {
            restart = true;
        }

        self.conf = req;
        if restart && self.on {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (stop_signal_rx, publish_rx, tx) = self.handle.take().unwrap().await.unwrap();
            self.event_loop(stop_signal_rx, publish_rx, tx, self.conf.sink.clone())
                .await;
        }
        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        if self.ref_rules.len() > 0 || self.active_ref_rules.len() > 0 {
            // TODO
            return Err(common::error::HaliaError::ConfErr);
        }

        persistence::devices::modbus::delete_sink(device_id, &self.id).await?;
        match self.stop_signal_tx {
            Some(_) => self.stop().await,
            None => {}
        }

        Ok(())
    }

    pub fn pre_publish(&mut self, rule_id: &Uuid) {
        self.ref_rules.push(rule_id.clone());
    }

    pub fn publish(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        self.active_ref_rules.push(rule_id.clone());
        self.ref_rules.retain(|id| id != rule_id);
        self.publish_tx.as_ref().unwrap().clone()
    }

    pub fn pre_unpublish(&mut self, rule_id: &Uuid) {
        self.ref_rules.retain(|id| id != rule_id);
    }

    pub fn unpublish(&mut self, rule_id: &Uuid) {
        self.active_ref_rules.retain(|id| id != rule_id);
        self.ref_rules.push(rule_id.clone());
    }

    pub async fn start(&mut self, tx: mpsc::Sender<WritePointEvent>) {
        if self.on {
            return;
        } else {
            self.on = true;
        }

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (publish_tx, publish_rx) = mpsc::channel(16);
        self.publish_tx = Some(publish_tx);

        self.event_loop(stop_signal_rx, publish_rx, tx, self.conf.sink.clone())
            .await;
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut publish_rx: mpsc::Receiver<MessageBatch>,
        tx: mpsc::Sender<WritePointEvent>,
        sink_conf: SinkConf,
    ) {
        let handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, publish_rx, tx);
                    }

                    mb = publish_rx.recv() => {
                        if let Some(mb) = mb {
                            Sink::send_write_point_event(mb, &sink_conf, &tx).await;
                        }
                    }
                }
            }
        });
        self.handle = Some(handle);
    }

    pub async fn stop(&mut self) {
        if !self.on {
            return;
        } else {
            self.on = false;
        }

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

    async fn send_write_point_event(
        mut mb: MessageBatch,
        sink_conf: &SinkConf,
        tx: &mpsc::Sender<WritePointEvent>,
    ) {
        let message = match mb.take_one_message() {
            Some(message) => message,
            None => return,
        };

        let typ: Type = match sink_conf.typ.typ {
            SinkValueType::Const => match &sink_conf.typ.value {
                serde_json::Value::String(s) => match serde_json::from_str(s) {
                    Ok(typ) => typ,
                    Err(_) => return,
                },
                _ => return,
            },
            SinkValueType::Variable => match &sink_conf.typ.value {
                serde_json::Value::String(field) => match message.get_str(field) {
                    Some(s) => match Type::try_from(s.as_str()) {
                        Ok(typ) => typ,
                        Err(_) => return,
                    },
                    None => return,
                },
                _ => return,
            },
        };

        let single_endian: Option<Endian> = match &sink_conf.single_endian {
            Some(single_endian) => match single_endian.typ {
                SinkValueType::Const => match &single_endian.value {
                    serde_json::Value::String(s) => match serde_json::from_str(s) {
                        Ok(endian) => endian,
                        Err(_) => return,
                    },
                    _ => return,
                },
                SinkValueType::Variable => match &single_endian.value {
                    serde_json::Value::String(field) => match message.get_str(field) {
                        Some(s) => match serde_json::from_str(s) {
                            Ok(endian) => endian,
                            Err(_) => return,
                        },
                        None => return,
                    },
                    _ => return,
                },
            },
            None => None,
        };

        let double_endian = match &sink_conf.double_endian {
            Some(double_endian) => match double_endian.typ {
                SinkValueType::Const => match &double_endian.value {
                    serde_json::Value::String(s) => match serde_json::from_str(s) {
                        Ok(endian) => endian,
                        Err(_) => return,
                    },
                    _ => return,
                },
                SinkValueType::Variable => match &double_endian.value {
                    serde_json::Value::String(field) => match message.get_str(field) {
                        Some(s) => match serde_json::from_str(s) {
                            Ok(endian) => endian,
                            Err(_) => return,
                        },
                        None => return,
                    },
                    _ => return,
                },
            },
            None => None,
        };

        let len = match &sink_conf.len {
            Some(len) => match len.typ {
                SinkValueType::Const => match common::json::get_u16(&len.value) {
                    Some(len) => Some(len),
                    None => return,
                },
                SinkValueType::Variable => match &len.value {
                    serde_json::Value::String(field) => match message.get_u16(field) {
                        Some(len) => Some(len),
                        None => return,
                    },
                    _ => return,
                },
            },
            None => None,
        };

        let single = match &sink_conf.single {
            Some(single) => match single.typ {
                SinkValueType::Const => match single.value.as_bool() {
                    Some(bool) => Some(bool),
                    None => return,
                },
                SinkValueType::Variable => match &single.value {
                    serde_json::Value::String(s) => match message.get_bool(s) {
                        Some(bool) => Some(bool),
                        None => return,
                    },
                    _ => return,
                },
            },
            None => None,
        };

        let pos = match &sink_conf.pos {
            Some(pos) => match pos.typ {
                SinkValueType::Const => match common::json::get_u8(&pos.value) {
                    Some(u8) => Some(u8),
                    None => return,
                },
                SinkValueType::Variable => match &pos.value {
                    serde_json::Value::String(field) => match message.get_u8(field) {
                        Some(u8) => Some(u8),
                        None => return,
                    },
                    _ => return,
                },
            },
            None => None,
        };

        let slave = match sink_conf.slave.typ {
            SinkValueType::Const => match common::json::get_u8(&sink_conf.slave.value) {
                Some(n) => n,
                None => {
                    return;
                }
            },
            SinkValueType::Variable => match &sink_conf.slave.value {
                serde_json::Value::String(field) => match message.get_u8(field) {
                    Some(n) => n,
                    None => return,
                },
                _ => {
                    return;
                }
            },
        };

        let area: Area = match sink_conf.area.typ {
            SinkValueType::Const => match &sink_conf.area.value {
                serde_json::Value::String(s) => match Area::try_from(s.as_str()) {
                    Ok(area) => area,
                    Err(_) => return,
                },
                _ => return,
            },
            SinkValueType::Variable => match &sink_conf.area.value {
                serde_json::Value::String(field) => match message.get_str(field) {
                    Some(s) => match Area::try_from(s.as_str()) {
                        Ok(area) => area,
                        Err(_) => return,
                    },
                    None => return,
                },
                _ => return,
            },
        };
        debug!("{:?}", area);

        let address = match sink_conf.address.typ {
            SinkValueType::Const => match common::json::get_u16(&sink_conf.address.value) {
                Some(n) => n,
                None => return,
            },
            SinkValueType::Variable => match &sink_conf.address.value {
                serde_json::Value::String(field) => match message.get_u16(&field) {
                    Some(address) => address,
                    None => return,
                },
                _ => return,
            },
        };
        debug!("{:?}", address);

        let value = match sink_conf.value.typ {
            SinkValueType::Const => sink_conf.value.value.clone(),
            SinkValueType::Variable => match &sink_conf.value.value {
                serde_json::Value::String(field) => match message.get(field) {
                    Some(v) => v.clone().into(),
                    None => return,
                },
                _ => return,
            },
        };
        debug!("{:?}", value);

        let data_type = DataType {
            typ,
            single_endian,
            double_endian,
            len,
            single,
            pos,
        };

        match WritePointEvent::new(slave, area, address, data_type, value) {
            Ok(wpe) => {
                tx.send(wpe).await.unwrap();
            }
            Err(e) => {
                debug!("value is err :{e}");
            }
        }
    }
}

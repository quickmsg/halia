use common::{error::HaliaResult, persistence};
use message::MessageBatch;
use tokio::{select, sync::mpsc, task::JoinHandle};
use tracing::{debug, warn};
use types::devices::modbus::{
    Area, CreateUpdateSinkReq, DataType, Endian, SearchSinksItemResp, SinkConf, Type,
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
        if self.conf.conf != req.conf {
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
            self.event_loop(stop_signal_rx, publish_rx, tx, self.conf.conf.clone())
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

        self.event_loop(stop_signal_rx, publish_rx, tx, self.conf.conf.clone())
            .await;
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut publish_rx: mpsc::Receiver<MessageBatch>,
        tx: mpsc::Sender<WritePointEvent>,
        conf: SinkConf,
    ) {
        let handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, publish_rx, tx);
                    }

                    mb = publish_rx.recv() => {
                        if let Some(mb) = mb {
                            Sink::send_write_point_event(mb, &conf, &tx).await;
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
        conf: &SinkConf,
        tx: &mpsc::Sender<WritePointEvent>,
    ) {
        debug!("{:?}\n{:?}", mb, conf);
        let message = match mb.take_one_message() {
            Some(message) => message,
            None => return,
        };

        let typ: Type = match conf.typ.typ {
            types::devices::SinkValueType::Const => match &conf.typ.value {
                serde_json::Value::String(s) => match serde_json::from_str(s) {
                    Ok(typ) => typ,
                    Err(_) => return,
                },
                _ => return,
            },
            types::devices::SinkValueType::Variable => match &conf.typ.value {
                serde_json::Value::String(field) => match message.get_str(field) {
                    Some(s) => {
                        debug!("{:?}", s);
                        match Type::try_from(s.as_str()) {
                            Ok(typ) => typ,
                            Err(_) => return,
                        }
                    }
                    None => return,
                },
                _ => return,
            },
        };
        debug!("{:?}", typ);

        let endian0: Option<Endian> = match &conf.endian0 {
            Some(endian0) => match endian0.typ {
                types::devices::SinkValueType::Const => match &endian0.value {
                    serde_json::Value::String(s) => match serde_json::from_str(s) {
                        Ok(endian) => endian,
                        Err(_) => return,
                    },
                    _ => return,
                },
                types::devices::SinkValueType::Variable => match &endian0.value {
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
        debug!("{:?}", endian0);

        let endian1 = match &conf.endian1 {
            Some(endian1) => match endian1.typ {
                types::devices::SinkValueType::Const => match &endian1.value {
                    serde_json::Value::String(s) => match serde_json::from_str(s) {
                        Ok(endian) => endian,
                        Err(_) => return,
                    },
                    _ => return,
                },
                types::devices::SinkValueType::Variable => match &endian1.value {
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
        debug!("{:?}", endian1);

        let len = match &conf.len {
            Some(len) => match len.typ {
                types::devices::SinkValueType::Const => match common::json::get_u16(&len.value) {
                    Some(len) => Some(len),
                    None => return,
                },
                types::devices::SinkValueType::Variable => match &len.value {
                    serde_json::Value::String(field) => match message.get_u16(field) {
                        Some(len) => Some(len),
                        None => return,
                    },
                    _ => return,
                },
            },
            None => None,
        };
        debug!("{:?}", len);

        let single = match &conf.single {
            Some(single) => match single.typ {
                types::devices::SinkValueType::Const => match single.value.as_bool() {
                    Some(bool) => Some(bool),
                    None => return,
                },
                types::devices::SinkValueType::Variable => match &single.value {
                    serde_json::Value::String(s) => match message.get_bool(s) {
                        Some(bool) => Some(bool),
                        None => return,
                    },
                    _ => return,
                },
            },
            None => None,
        };
        debug!("{:?}", single);

        let pos = match &conf.pos {
            Some(pos) => match pos.typ {
                types::devices::SinkValueType::Const => match common::json::get_u8(&pos.value) {
                    Some(u8) => Some(u8),
                    None => return,
                },
                types::devices::SinkValueType::Variable => match &pos.value {
                    serde_json::Value::String(field) => match message.get_u8(field) {
                        Some(u8) => Some(u8),
                        None => return,
                    },
                    _ => return,
                },
            },
            None => None,
        };
        debug!("{:?}", pos);

        let slave = match conf.slave.typ {
            types::devices::SinkValueType::Const => match common::json::get_u8(&conf.slave.value) {
                Some(n) => n,
                None => {
                    warn!("slave只能是number类型");
                    return;
                }
            },
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
        debug!("{:?}", slave);

        let area: Area = match conf.area.typ {
            types::devices::SinkValueType::Const => match &conf.area.value {
                serde_json::Value::String(s) => match Area::try_from(s.as_str()) {
                    Ok(area) => area,
                    Err(_) => return,
                },
                _ => return,
            },
            types::devices::SinkValueType::Variable => match &conf.area.value {
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

        let address = match conf.address.typ {
            types::devices::SinkValueType::Const => {
                match common::json::get_u16(&conf.address.value) {
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
        debug!("{:?}", address);

        let value = match conf.value.typ {
            types::devices::SinkValueType::Const => conf.value.value.clone(),
            types::devices::SinkValueType::Variable => match &conf.value.value {
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
            endian0,
            endian1,
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

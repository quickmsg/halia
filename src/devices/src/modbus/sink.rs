use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use tokio::{
    select,
    sync::{mpsc, RwLock},
    task::JoinHandle,
};
use tracing::debug;
use types::{
    devices::modbus::{CreateUpdateSinkReq, SearchSinksItemResp, SinkConf},
    TargetValueType,
};
use uuid::Uuid;

use super::WritePointEvent;

#[derive(Debug)]
pub struct Sink {
    pub id: Uuid,
    conf: CreateUpdateSinkReq,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
            mpsc::Sender<WritePointEvent>,
            Arc<RwLock<Option<String>>>,
        )>,
    >,

    ref_info: RefInfo,
    mb_tx: Option<mpsc::Sender<MessageBatch>>,
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
            stop_signal_tx: None,
            join_handle: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
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

        let mut restart = false;
        if self.conf.sink != req.sink {
            restart = true;
        }

        self.conf = req;
        if restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (stop_signal_rx, publish_rx, tx, device_err) =
                self.join_handle.take().unwrap().await.unwrap();
            self.event_loop(
                stop_signal_rx,
                publish_rx,
                tx,
                self.conf.sink.clone(),
                device_err,
            )
            .await;
        }
        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::Common("该动作含有引用规则".to_owned()));
        }

        persistence::devices::modbus::delete_sink(device_id, &self.id).await?;
        match self.stop_signal_tx {
            Some(_) => self.stop().await,
            None => {}
        }

        Ok(())
    }

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.add_ref(rule_id);
    }

    pub fn get_mb_tx(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.mb_tx.as_ref().unwrap().clone()
    }

    pub fn del_mb_tx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id);
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.del_ref(rule_id);
    }

    pub async fn start(
        &mut self,
        device_tx: mpsc::Sender<WritePointEvent>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(mb_tx);

        self.event_loop(
            stop_signal_rx,
            mb_rx,
            device_tx,
            self.conf.sink.clone(),
            device_err,
        )
        .await;
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        device_tx: mpsc::Sender<WritePointEvent>,
        sink_conf: SinkConf,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, mb_rx, device_tx, device_err);
                    }

                    mb = mb_rx.recv() => {
                        if let Some(mb) = mb {
                            if device_err.read().await.is_none() {
                                Sink::send_write_point_event(mb, &sink_conf, &device_tx).await;
                            }
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;
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

        // let typ: Type = match sink_conf.typ.typ {
        //     TargetValueType::Const => match &sink_conf.typ.value {
        //         serde_json::Value::String(s) => match serde_json::from_str(s) {
        //             Ok(typ) => typ,
        //             Err(_) => return,
        //         },
        //         _ => return,
        //     },
        //     TargetValueType::Variable => match &sink_conf.typ.value {
        //         serde_json::Value::String(field) => match message.get_str(field) {
        //             Some(s) => match Type::try_from(s.as_str()) {
        //                 Ok(typ) => typ,
        //                 Err(_) => return,
        //             },
        //             None => return,
        //         },
        //         _ => return,
        //     },
        // };

        // let single_endian: Option<Endian> = match &sink_conf.single_endian {
        //     Some(single_endian) => match single_endian.typ {
        //         TargetValueType::Const => match &single_endian.value {
        //             serde_json::Value::String(s) => match serde_json::from_str(s) {
        //                 Ok(endian) => endian,
        //                 Err(_) => return,
        //             },
        //             _ => return,
        //         },
        //         TargetValueType::Variable => match &single_endian.value {
        //             serde_json::Value::String(field) => match message.get_str(field) {
        //                 Some(s) => match serde_json::from_str(s) {
        //                     Ok(endian) => endian,
        //                     Err(_) => return,
        //                 },
        //                 None => return,
        //             },
        //             _ => return,
        //         },
        //     },
        //     None => None,
        // };

        // let double_endian = match &sink_conf.double_endian {
        //     Some(double_endian) => match double_endian.typ {
        //         TargetValueType::Const => match &double_endian.value {
        //             serde_json::Value::String(s) => match serde_json::from_str(s) {
        //                 Ok(endian) => endian,
        //                 Err(_) => return,
        //             },
        //             _ => return,
        //         },
        //         TargetValueType::Variable => match &double_endian.value {
        //             serde_json::Value::String(field) => match message.get_str(field) {
        //                 Some(s) => match serde_json::from_str(s) {
        //                     Ok(endian) => endian,
        //                     Err(_) => return,
        //                 },
        //                 None => return,
        //             },
        //             _ => return,
        //         },
        //     },
        //     None => None,
        // };

        // let len = match &sink_conf.len {
        //     Some(len) => match len.typ {
        //         TargetValueType::Const => match common::json::get_u16(&len.value) {
        //             Some(len) => Some(len),
        //             None => return,
        //         },
        //         TargetValueType::Variable => match &len.value {
        //             serde_json::Value::String(field) => match message.get_u16(field) {
        //                 Some(len) => Some(len),
        //                 None => return,
        //             },
        //             _ => return,
        //         },
        //     },
        //     None => None,
        // };

        // let single = match &sink_conf.single {
        //     Some(single) => match single.typ {
        //         TargetValueType::Const => match single.value.as_bool() {
        //             Some(bool) => Some(bool),
        //             None => return,
        //         },
        //         TargetValueType::Variable => match &single.value {
        //             serde_json::Value::String(s) => match message.get_bool(s) {
        //                 Some(bool) => Some(bool),
        //                 None => return,
        //             },
        //             _ => return,
        //         },
        //     },
        //     None => None,
        // };

        // let pos = match &sink_conf.pos {
        //     Some(pos) => match pos.typ {
        //         TargetValueType::Const => match common::json::get_u8(&pos.value) {
        //             Some(u8) => Some(u8),
        //             None => return,
        //         },
        //         TargetValueType::Variable => match &pos.value {
        //             serde_json::Value::String(field) => match message.get_u8(field) {
        //                 Some(u8) => Some(u8),
        //                 None => return,
        //             },
        //             _ => return,
        //         },
        //     },
        //     None => None,
        // };

        // let slave = match sink_conf.slave.typ {
        //     TargetValueType::Const => match common::json::get_u8(&sink_conf.slave.value) {
        //         Some(n) => n,
        //         None => {
        //             return;
        //         }
        //     },
        //     TargetValueType::Variable => match &sink_conf.slave.value {
        //         serde_json::Value::String(field) => match message.get_u8(field) {
        //             Some(n) => n,
        //             None => return,
        //         },
        //         _ => {
        //             return;
        //         }
        //     },
        // };

        // let area: Area = match sink_conf.area.typ {
        //     TargetValueType::Const => match &sink_conf.area.value {
        //         serde_json::Value::String(s) => match Area::try_from(s.as_str()) {
        //             Ok(area) => area,
        //             Err(_) => return,
        //         },
        //         _ => return,
        //     },
        //     TargetValueType::Variable => match &sink_conf.area.value {
        //         serde_json::Value::String(field) => match message.get_str(field) {
        //             Some(s) => match Area::try_from(s.as_str()) {
        //                 Ok(area) => area,
        //                 Err(_) => return,
        //             },
        //             None => return,
        //         },
        //         _ => return,
        //     },
        // };
        // debug!("{:?}", area);

        // let address = match sink_conf.address.typ {
        //     TargetValueType::Const => match common::json::get_u16(&sink_conf.address.value) {
        //         Some(n) => n,
        //         None => return,
        //     },
        //     TargetValueType::Variable => match &sink_conf.address.value {
        //         serde_json::Value::String(field) => match message.get_u16(&field) {
        //             Some(address) => address,
        //             None => return,
        //         },
        //         _ => return,
        //     },
        // };
        // debug!("{:?}", address);

        let value = match sink_conf.value.typ {
            TargetValueType::Const => sink_conf.value.value.clone(),
            TargetValueType::Variable => match &sink_conf.value.value {
                serde_json::Value::String(field) => match message.get(field) {
                    Some(v) => v.clone().into(),
                    None => return,
                },
                _ => return,
            },
        };
        debug!("{:?}", value);

        match WritePointEvent::new(
            sink_conf.slave,
            sink_conf.area,
            sink_conf.address,
            sink_conf.data_type,
            value,
        ) {
            Ok(wpe) => {
                tx.send(wpe).await.unwrap();
            }
            Err(e) => {
                debug!("value is err :{e}");
            }
        }
    }

    pub fn can_stop(&self) -> bool {
        self.ref_info.can_stop()
    }

    pub fn can_delete(&self) -> bool {
        self.ref_info.can_delete()
    }
}

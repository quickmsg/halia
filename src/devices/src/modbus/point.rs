use std::{io, sync::Arc, time::Duration};

use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::{
    error::{HaliaError, HaliaResult},
    get_id, persistence,
    ref_info::RefInfo,
};
use message::{Message, MessageBatch};
use protocol::modbus::Context;
use serde_json::Value;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::{debug, warn};
use types::devices::modbus::{Area, CreateUpdatePointReq, SearchPointsItemResp};
use uuid::Uuid;

#[derive(Debug)]
pub struct Point {
    pub id: Uuid,
    pub conf: CreateUpdatePointReq,
    quantity: u16,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Sender<Uuid>,
            Arc<RwLock<Option<String>>>,
        )>,
    >,
    value: serde_json::Value,
    err_info: Option<String>,

    ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Point {
    pub async fn new(
        device_id: &Uuid,
        point_id: Option<Uuid>,
        req: CreateUpdatePointReq,
    ) -> HaliaResult<Point> {
        let (point_id, new) = get_id(point_id);

        let quantity = req.ext.data_type.get_quantity();

        if new {
            persistence::devices::modbus::create_point(
                device_id,
                &point_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Point {
            id: point_id,
            conf: req,
            quantity,
            value: Value::Null,
            stop_signal_tx: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
            join_handle: None,
            err_info: None,
        })
    }

    pub fn search(&self) -> SearchPointsItemResp {
        SearchPointsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            value: self.value.clone(),
            err_info: self.err_info.clone(),
            active_ref_rule_cnt: self.ref_info.active_ref_cnt(),
            ref_rule_cnt: self.ref_info.ref_cnt(),
        }
    }

    pub async fn start(
        &mut self,
        read_tx: mpsc::Sender<Uuid>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        self.event_loop(self.conf.ext.interval, stop_signal_rx, read_tx, device_err)
            .await;
    }

    async fn event_loop(
        &mut self,
        interval: u64,
        mut stop_signal_rx: mpsc::Receiver<()>,
        read_tx: mpsc::Sender<Uuid>,
        device_err: Arc<RwLock<Option<String>>>,
    ) {
        let point_id = self.id.clone();
        let join_handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, read_tx, device_err);
                    }

                    _ = interval.tick() => {
                        if device_err.read().await.is_none() {
                            if let Err(e) = read_tx.send(point_id).await {
                                debug!("send point info err :{}", e);
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

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdatePointReq) -> HaliaResult<()> {
        persistence::devices::modbus::update_point(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.quantity = req.ext.data_type.get_quantity();
        self.conf = req;

        if self.stop_signal_tx.is_some() && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (stop_signal_rx, read_tx, device_err) =
                self.join_handle.take().unwrap().await.unwrap();
            self.event_loop(self.conf.ext.interval, stop_signal_rx, read_tx, device_err)
                .await;
        }

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::Common("引用中".to_owned()));
        }
        self.stop().await;
        persistence::devices::modbus::delete_point(device_id, &self.id).await?;
        Ok(())
    }

    pub async fn read(&mut self, ctx: &mut Box<dyn Context>) -> io::Result<()> {
        debug!("read point {:?}", self);
        let res = match self.conf.ext.area {
            Area::InputDiscrete => {
                ctx.read_discrete_inputs(self.conf.ext.slave, self.conf.ext.address, self.quantity)
                    .await
            }
            Area::Coils => {
                ctx.read_coils(self.conf.ext.slave, self.conf.ext.address, self.quantity)
                    .await
            }
            Area::InputRegisters => {
                ctx.read_input_registers(self.conf.ext.slave, self.conf.ext.address, self.quantity)
                    .await
            }
            Area::HoldingRegisters => {
                ctx.read_holding_registers(
                    self.conf.ext.slave,
                    self.conf.ext.address,
                    self.quantity,
                )
                .await
            }
        };

        match res {
            Ok(mut data) => {
                let value = self.conf.ext.data_type.decode(&mut data);
                debug!("{:?}", value);
                match &value {
                    message::MessageValue::Bytes(bytes) => {
                        let str = BASE64_STANDARD.encode(bytes);
                        self.value = serde_json::Value::String(str);
                    }
                    _ => self.value = value.clone().into(),
                }
                match &self.mb_tx {
                    Some(tx) => {
                        let mut message = Message::default();
                        message.add(self.conf.base.name.clone(), value);
                        let mut message_batch = MessageBatch::default();
                        message_batch.push_message(message);
                        if let Err(e) = tx.send(message_batch) {
                            warn!("send err :{:?}", e);
                        }
                    }
                    None => {}
                }
                Ok(())
            }
            Err(e) => match e {
                protocol::modbus::ModbusError::Transport(e) => Err(e),
                protocol::modbus::ModbusError::Protocol(e) => {
                    warn!("{}", e);
                    Ok(())
                }
                protocol::modbus::ModbusError::Exception(e) => {
                    warn!("{}", e);
                    Ok(())
                }
            },
        }
    }

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.add_ref(rule_id);
    }

    pub fn get_mb_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        match &self.mb_tx {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel(16);
                self.mb_tx = Some(tx);
                rx
            }
        }
    }

    pub fn del_mb_rx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id);
        if self.ref_info.can_stop() {
            self.mb_tx = None;
        }
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.del_ref(rule_id)
    }

    pub fn can_delete(&self) -> bool {
        self.ref_info.can_delete()
    }

    pub fn can_stop(&self) -> bool {
        self.ref_info.can_stop()
    }
}

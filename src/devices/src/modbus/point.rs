use std::time::Duration;

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::{Message, MessageBatch};
use protocol::modbus::{
    client::{Context, Reader},
    SlaveContext,
};
use serde_json::Value;
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};
use tracing::{debug, warn};
use types::devices::modbus::{Area, CreateUpdatePointReq, SearchPointsItemResp};
use uuid::Uuid;

#[derive(Debug)]
pub struct Point {
    pub id: Uuid,
    pub on: bool,
    pub conf: CreateUpdatePointReq,

    pub quantity: u16,
    pub value: Value,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    pub ref_rules: Vec<Uuid>,
    pub active_ref_rules: Vec<Uuid>,
    pub tx: Option<broadcast::Sender<MessageBatch>>,

    handle: Option<JoinHandle<(mpsc::Receiver<()>, mpsc::Sender<Uuid>)>>,
}

impl Point {
    pub async fn new(
        device_id: &Uuid,
        point_id: Option<Uuid>,
        req: CreateUpdatePointReq,
    ) -> HaliaResult<Point> {
        let (point_id, new) = match point_id {
            Some(point_id) => (point_id, false),
            None => (Uuid::new_v4(), true),
        };

        let quantity = match req.point.data_type.get_quantity() {
            Some(quantity) => quantity,
            None => return Err(HaliaError::ConfErr),
        };

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
            on: false,
            conf: req,
            quantity,
            value: Value::Null,
            stop_signal_tx: None,
            ref_rules: vec![],
            active_ref_rules: vec![],
            tx: None,
            handle: None,
        })
    }

    pub fn search(&self) -> SearchPointsItemResp {
        SearchPointsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            ref_rules: self.ref_rules.clone(),
            active_ref_rules: self.active_ref_rules.clone(),
            value: self.value.clone(),
        }
    }

    pub async fn start(&mut self, read_tx: mpsc::Sender<Uuid>) {
        if self.on {
            return;
        } else {
            self.on = true;
        }

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        self.event_loop(self.conf.point.interval, stop_signal_rx, read_tx)
            .await;
    }

    async fn event_loop(
        &mut self,
        interval: u64,
        mut stop_signal_rx: mpsc::Receiver<()>,
        read_tx: mpsc::Sender<Uuid>,
    ) {
        let point_id = self.id.clone();
        let handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, read_tx);
                    }

                    _ = interval.tick() => {
                        if let Err(e) = read_tx.send(point_id).await {
                            debug!("send point info err :{}", e);
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
            self.on = true;
        }

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
        if self.conf.point != req.point {
            restart = true;
        }
        self.quantity = match req.point.data_type.get_quantity() {
            Some(quantity) => quantity,
            None => return Err(HaliaError::ConfErr),
        };
        self.conf = req;

        if self.stop_signal_tx.is_some() && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (stop_signal_rx, read_tx) = self.handle.take().unwrap().await.unwrap();
            self.event_loop(self.conf.point.interval, stop_signal_rx, read_tx)
                .await;
        }

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        if self.ref_rules.len() > 0 || self.active_ref_rules.len() > 0 {
            // TODO 引用中
            return Err(HaliaError::ConfErr);
        }
        self.stop().await;
        persistence::devices::modbus::delete_point(device_id, &self.id).await?;
        Ok(())
    }

    pub async fn read(&mut self, ctx: &mut Context) -> HaliaResult<()> {
        let point_conf = &self.conf.point;
        ctx.set_slave(point_conf.slave);
        let message_value = match point_conf.area {
            Area::InputDiscrete => {
                match ctx
                    .read_discrete_inputs(point_conf.address, self.quantity)
                    .await
                {
                    Ok(res) => match res {
                        Ok(mut data) => {
                            let value = point_conf.data_type.decode(&mut data);
                            self.value = value.clone().into();
                            value
                        }
                        Err(e) => {
                            warn!("modbus protocl exception:{}", e);
                            return Ok(());
                        }
                    },
                    Err(_) => return Err(HaliaError::Disconnect),
                }
            }
            Area::Coils => match ctx.read_coils(point_conf.address, self.quantity).await {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = point_conf.data_type.decode(&mut data);
                        self.value = value.clone().into();
                        value
                    }
                    Err(e) => {
                        warn!("modbus protocl exception:{}", e);
                        return Ok(());
                    }
                },
                Err(_) => return Err(HaliaError::Disconnect),
            },
            Area::InputRegisters => {
                match ctx
                    .read_input_registers(point_conf.address, self.quantity)
                    .await
                {
                    Ok(res) => match res {
                        Ok(mut data) => {
                            let value = point_conf.data_type.decode(&mut data);
                            self.value = value.clone().into();
                            value
                        }
                        Err(_) => return Ok(()),
                    },
                    Err(_) => return Err(HaliaError::Disconnect),
                }
            }
            Area::HoldingRegisters => match ctx
                .read_holding_registers(point_conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = point_conf.data_type.decode(&mut data);
                        self.value = value.clone().into();
                        value
                    }
                    Err(_) => return Ok(()),
                },
                Err(_) => return Err(HaliaError::Disconnect),
            },
        };

        match &self.tx {
            Some(tx) => {
                let mut message = Message::default();
                message.add(self.conf.base.name.clone(), message_value);
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

    pub fn pre_subscribe(&mut self, rule_id: &Uuid) {
        self.ref_rules.push(rule_id.clone());
    }

    pub fn subscribe(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        self.ref_rules.retain(|id| id != rule_id);
        self.active_ref_rules.push(rule_id.clone());
        match &self.tx {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel(16);
                self.tx = Some(tx);
                rx
            }
        }
    }

    pub fn unsubscribe(&mut self, rule_id: &Uuid) {
        self.active_ref_rules.retain(|id| id != rule_id);
        self.ref_rules.push(rule_id.clone());
    }

    pub fn after_unsubscribe(&mut self, rule_id: &Uuid) {
        self.ref_rules.retain(|id| id != rule_id);
    }
}

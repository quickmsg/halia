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
    time,
};
use tracing::{debug, warn};
use types::devices::modbus::{Area, CreateUpdatePointReq, SearchPointsItemResp};
use uuid::Uuid;

#[derive(Debug)]
pub struct Point {
    pub id: Uuid,
    pub conf: CreateUpdatePointReq,

    pub quantity: u16,
    pub value: Value,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,
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

        let quantity = match req.data_type.get_quantity() {
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
            conf: req,
            quantity,
            value: Value::Null,
            stop_signal_tx: None,
            tx: None,
            ref_cnt: 0,
        })
    }

    pub fn search(&self) -> SearchPointsItemResp {
        SearchPointsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub fn start(&mut self, read_tx: mpsc::Sender<Uuid>) {
        if self.stop_signal_tx.is_some() {
            return;
        }

        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let interval = self.conf.interval;
        let point_id = self.id.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    _ = stop_signal_rx.recv() => {
                        return
                    }

                    _ = interval.tick() => {
                        if let Err(e) = read_tx.send(point_id).await {
                            debug!("send point info err :{}", e);
                        }
                    }
                }
            }
        });
    }

    pub async fn stop(&mut self) {
        if self.stop_signal_tx.is_none() {
            return;
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

        self.quantity = match req.data_type.get_quantity() {
            Some(quantity) => quantity,
            None => return Err(HaliaError::ConfErr),
        };
        self.conf = req;

        Ok(())
    }

    pub async fn delete(&self, device_id: &Uuid) -> HaliaResult<()> {
        // TODO stop
        persistence::devices::modbus::delete_point(device_id, &self.id).await?;
        Ok(())
    }

    pub async fn read(&mut self, ctx: &mut Context) -> HaliaResult<()> {
        ctx.set_slave(self.conf.slave);
        let message_value = match self.conf.area {
            Area::InputDiscrete => match ctx
                .read_discrete_inputs(self.conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.data_type.decode(&mut data);
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
            Area::Coils => match ctx.read_coils(self.conf.address, self.quantity).await {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.data_type.decode(&mut data);
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
            Area::InputRegisters => match ctx
                .read_input_registers(self.conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.data_type.decode(&mut data);
                        self.value = value.clone().into();
                        value
                    }
                    Err(_) => return Ok(()),
                },
                Err(_) => return Err(HaliaError::Disconnect),
            },
            Area::HoldingRegisters => match ctx
                .read_holding_registers(self.conf.address, self.quantity)
                .await
            {
                Ok(res) => match res {
                    Ok(mut data) => {
                        let value = self.conf.data_type.decode(&mut data);
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
                message.add(self.conf.name.clone(), message_value);
                let mut message_batch = MessageBatch::default();
                message_batch.push_message(message);
                tx.send(message_batch);
            }
            None => {}
        }

        Ok(())
    }

    pub fn subscribe(&mut self) -> broadcast::Receiver<MessageBatch> {
        self.ref_cnt += 1;
        match &self.tx {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel::<MessageBatch>(16);
                self.tx = Some(tx);
                rx
            }
        }
    }

    pub fn unsubscribe(&mut self) {
        self.ref_cnt -= 1;
        if self.ref_cnt == 0 {
            self.tx = None;
        }
    }
}

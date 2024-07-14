use anyhow::{bail, Result};
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::{Message, MessageBatch};
use protocol::modbus::client::Context;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{broadcast, mpsc},
    time,
};
use tracing::debug;
use types::device::point::{SearchPointItemResp, SearchPointResp};
use uuid::Uuid;

use super::{group_point::Point, WritePointEvent};

#[derive(Debug)]
pub struct Group {
    pub id: Uuid,
    pub conf: Conf,

    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    pub points: Vec<Point>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Conf {
    pub name: String,
    pub interval: u64,
    pub desc: Option<String>,
}

#[derive(Serialize)]
pub struct SearchInfo {
    pub id: Uuid,
    pub conf: Conf,
    pub point_cnt: usize,
}

impl Group {
    pub async fn new(device_id: &Uuid, group_id: Option<Uuid>, data: String) -> Result<Self> {
        let (group_id, new) = match group_id {
            Some(group_id) => (group_id, false),
            None => (Uuid::new_v4(), true),
        };

        let conf: Conf = serde_json::from_str(&data)?;
        if conf.interval == 0 {
            bail!("group interval must > 0")
        }

        if new {
            persistence::modbus::create_group(device_id, &group_id, &data).await?;
        }

        Ok(Group {
            id: group_id,
            conf,
            points: vec![],
            tx: None,
            ref_cnt: 0,
            stop_signal_tx: None,
        })
    }

    pub async fn recover(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        match persistence::modbus::read_group_points(device_id, &self.id).await {
            Ok(points) => {
                for (point_id, data) in points {
                    self.create_point(device_id, Some(point_id), data).await?;
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn search(&self) -> serde_json::Value {
        json!(SearchInfo {
            id: self.id.clone(),
            conf: self.conf.clone(),
            point_cnt: self.points.len(),
        })
    }

    pub fn start(&mut self, read_tx: mpsc::Sender<Uuid>, err: Arc<AtomicBool>) {
        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let interval = self.conf.interval;
        let group_id = self.id.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    _ = stop_signal_rx.recv() => {
                        debug!("group stop");
                        return
                    }

                    _ = interval.tick() => {
                        if !err.load(Ordering::SeqCst) {
                            if let Err(e) = read_tx.send(group_id).await {
                                debug!("group send point info err :{}", e);
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn stop(&mut self) {
        match self.stop_signal_tx.as_ref().unwrap().send(()).await {
            Ok(()) => debug!("send stop signal ok"),
            Err(e) => debug!("send stop signal err :{e:?}"),
        }
        self.stop_signal_tx = None;
    }

    pub async fn update(&mut self, device_id: &Uuid, data: String) -> HaliaResult<bool> {
        let update_conf: Conf = serde_json::from_str(&data)?;

        persistence::modbus::update_group(device_id, &self.id, &data).await?;

        let mut restart = false;
        if self.conf.interval != update_conf.interval {
            restart = true;
        }
        self.conf = update_conf;

        Ok(restart)
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        match self.stop_signal_tx {
            Some(_) => self.stop().await,
            None => {}
        }
        for point in &self.points {
            point.delete(device_id, &self.id).await?;
        }

        persistence::modbus::delete_group(device_id, &self.id).await?;

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

    pub async fn create_point(
        &mut self,
        device_id: &Uuid,
        point_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        let point = Point::new(device_id, &self.id, point_id, data).await?;
        self.points.push(point);
        Ok(())
    }

    pub async fn search_points(&self, page: usize, size: usize) -> SearchPointResp {
        let mut resps = vec![];
        for point in self.points.iter().rev().skip((page - 1) * size) {
            resps.push(SearchPointItemResp {
                id: point.id.clone(),
                name: point.conf.name.clone(),
                conf: serde_json::json!(point.conf),
                value: point.value.clone(),
            });
            if resps.len() == size {
                break;
            }
        }

        SearchPointResp {
            total: self.points.len(),
            data: resps,
        }
    }

    pub async fn update_point(
        &mut self,
        device_id: &Uuid,
        point_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        match self.points.iter_mut().find(|point| point.id == point_id) {
            Some(point) => point.update(device_id, &self.id, data).await,
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_points(
        &mut self,
        device_id: &Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        for point_id in &point_ids {
            if let Some(point) = self.points.iter().find(|point| point.id == *point_id) {
                point.delete(device_id, &self.id).await?;
            }
        }
        self.points.retain(|point| !point_ids.contains(&point.id));
        Ok(())
    }

    pub async fn read_points_value(
        &mut self,
        ctx: &mut Context,
        interval: u64,
        rtt: &Arc<AtomicU16>,
    ) -> Result<()> {
        let mut msg = Message::default();
        for point in self.points.iter_mut() {
            let now = Instant::now();
            match point.read(ctx).await {
                Ok(data) => {
                    msg.add(point.conf.name.clone(), data);
                }
                Err(e) => bail!("连接断开"),
            }
            rtt.store(now.elapsed().as_millis() as u16, Ordering::SeqCst);
            time::sleep(Duration::from_millis(interval)).await;
        }

        if let Some(tx) = &self.tx {
            let mut mb = MessageBatch::default();
            mb.push_message(msg);
            if let Err(e) = tx.send(mb) {
                debug!("unscribe :{}", e);
                self.tx = None;
            }
        }

        Ok(())
    }

    pub async fn get_write_point_event(
        &self,
        point_id: Uuid,
        value: String,
    ) -> HaliaResult<WritePointEvent> {
        let value: serde_json::Value = serde_json::from_str(&value)?;
        match self.points.iter().find(|point| point.id == point_id) {
            Some(point) => WritePointEvent::new(
                point.conf.slave,
                point.conf.area.clone(),
                point.conf.address,
                point.conf.r#type.clone(),
                value,
            ),
            None => Err(HaliaError::NotFound),
        }
    }
}

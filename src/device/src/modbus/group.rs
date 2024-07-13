use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use message::{Message, MessageBatch};
use protocol::modbus::client::Context;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tracing::debug;
use types::device::{
    group::{CreateGroupReq, UpdateGroupReq},
    point::{CreatePointReq, SearchPointItemResp, SearchPointResp},
};
use uuid::Uuid;

use super::{point::Point, WritePointEvent};

#[derive(Debug)]
pub struct Group {
    pub id: Uuid,
    pub name: String,
    pub interval: u64,
    pub points: RwLock<Vec<Point>>,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,
    pub desc: Option<String>,

    pub stop_signal_tx: Option<mpsc::Sender<()>>,
}

impl Group {
    pub fn new(group_id: Uuid, conf: &CreateGroupReq) -> Result<Self> {
        if conf.interval == 0 {
            bail!("group interval must > 0")
        }
        Ok(Group {
            id: group_id,
            name: conf.name.clone(),
            interval: conf.interval,
            points: RwLock::new(vec![]),
            tx: None,
            ref_cnt: 0,
            desc: conf.desc.clone(),
            stop_signal_tx: None,
        })
    }

    pub fn start(&mut self, read_tx: mpsc::Sender<Uuid>, err: Arc<AtomicBool>) {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(tx);
        let interval = self.interval;
        let group_id = self.id.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    _ = rx.recv() => {
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

    pub fn update(&mut self, req: &UpdateGroupReq) -> HaliaResult<bool> {
        self.name = req.name.clone();
        self.interval = req.interval;
        Ok(true)
    }

    pub fn subscribe(&mut self) -> broadcast::Receiver<MessageBatch> {
        self.ref_cnt += 1;
        match &self.tx {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel::<MessageBatch>(20);
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

    pub async fn create_point(&self, point_id: Uuid, req: CreatePointReq) -> HaliaResult<()> {
        let point = Point::new(req.clone(), point_id)?;
        self.points.write().await.push(point);
        Ok(())
    }

    pub async fn search_points(&self, page: usize, size: usize) -> SearchPointResp {
        let mut resps = vec![];
        for point in self
            .points
            .read()
            .await
            .iter()
            .rev()
            .skip((page - 1) * size)
        {
            resps.push(SearchPointItemResp {
                id: point.id.clone(),
                name: point.name.clone(),
                conf: serde_json::json!(point.conf),
                value: point.value.clone(),
            });
            if resps.len() == size {
                break;
            }
        }

        SearchPointResp {
            total: self.points.read().await.len(),
            data: resps,
        }
    }

    pub async fn update_point(&self, point_id: Uuid, req: &CreatePointReq) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == point_id)
        {
            Some(point) => point.update(req).await,
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn get_points_num(&self) -> usize {
        self.points.read().await.len()
    }

    pub async fn delete_points(&self, ids: &Vec<Uuid>) {
        self.points
            .write()
            .await
            .retain(|point| !ids.contains(&point.id));
    }

    pub async fn read_points_value(
        &mut self,
        ctx: &mut Context,
        interval: u64,
        rtt: &Arc<AtomicU16>,
    ) -> Result<()> {
        let mut msg = Message::default();
        for point in self.points.write().await.iter_mut() {
            let now = Instant::now();
            match point.read(ctx).await {
                Ok(data) => {
                    msg.add(point.name.clone(), data);
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
        value: serde_json::Value,
    ) -> HaliaResult<WritePointEvent> {
        match self
            .points
            .read()
            .await
            .iter()
            .find(|point| point.id == point_id)
        {
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

use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use message::{Message, MessageBatch};
use protocol::modbus::client::Context;
use std::{
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tracing::{debug, error};
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
}

#[derive(Clone)]
pub(crate) enum Command {
    Stop(Uuid),
    Pause,
    Restart,
    Update(Uuid, u64),
    StopAll,
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
        })
    }

    pub fn run(&self, mut cmd_rx: broadcast::Receiver<Command>, read_tx: mpsc::Sender<Uuid>) {
        let interval = self.interval;
        let group_id = self.id;
        tokio::spawn(async move {
            let mut pause = false;
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    signal = cmd_rx.recv() => {
                        match signal {
                            Ok(cmd) => {
                                match cmd {
                                    Command::Stop(id) =>  if id == group_id {
                                        debug!("group {} stop.", group_id);
                                        return
                                    }
                                    Command::Pause => pause = true,
                                    Command::Restart => pause = false,
                                    Command::Update(id, duraion) => {
                                        if id == group_id {
                                            interval = time::interval(Duration::from_millis(duraion));
                                        }
                                    }
                                    Command::StopAll => {
                                        debug!("group {} stop.", group_id);
                                        return
                                    }
                                }
                            }
                            Err(e) => error!("group recv cmd signal err :{:?}", e),
                        }
                    }

                    _ = interval.tick() => {
                        if !pause {
                            if let Err(e) = read_tx.send(group_id).await {
                                debug!("group send point info err :{}", e);
                            }
                        }
                    }
                }
            }
        });
    }

    pub fn update(&mut self, req: &UpdateGroupReq) {
        self.name = req.name.clone();
        self.interval = req.interval;
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
    ) -> Result<WritePointEvent> {
        match self
            .points
            .read()
            .await
            .iter()
            .find(|point| point.id == point_id)
        {
            Some(point) => Ok(WritePointEvent {
                slave: point.conf.slave,
                area: point.conf.area.clone(),
                address: point.conf.address,
                data_type: point.conf.r#type.clone(),
                value: value,
            }),
            None => bail!("not found"),
        }
    }
}

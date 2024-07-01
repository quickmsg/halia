use anyhow::{bail, Result};
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::{Message, MessageBatch};
use protocol::modbus::client::Context;
use std::{
    collections::HashMap,
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

use super::point::Point;

#[derive(Debug)]
pub(crate) struct Group {
    pub id: Uuid,
    pub name: String,
    pub interval: u64,
    pub points: RwLock<HashMap<Uuid, Point>>,
    pub device_id: Uuid,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,
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
    pub fn new(device_id: Uuid, group_id: Uuid, conf: &CreateGroupReq) -> Result<Self> {
        if conf.interval == 0 {
            bail!("group interval must > 0")
        }
        Ok(Group {
            id: group_id,
            device_id,
            name: conf.name.clone(),
            interval: conf.interval,
            points: RwLock::new(HashMap::new()),
            tx: None,
            ref_cnt: 0,
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

    pub async fn create_point(
        &self,
        point_id: Option<Uuid>,
        req: CreatePointReq,
    ) -> HaliaResult<()> {
        let (point_id, create) = match point_id {
            Some(point_id) => (point_id, false),
            None => (Uuid::new_v4(), true),
        };

        let point = Point::new(req.clone(), point_id)?;
        self.points.write().await.insert(point_id, point);
        if create {
            if let Err(e) = persistence::point::insert(
                &self.device_id,
                &self.id,
                &point_id,
                &serde_json::to_string(&req)?,
            )
            .await
            {
                error!("insert point err:{}", e);
            }
        }

        Ok(())
    }

    pub async fn search_point(&self, page: usize, size: usize) -> SearchPointResp {
        let mut resps = vec![];
        for (_, point) in self.points.read().await.iter().skip((page - 1) * size) {
            resps.push(SearchPointItemResp {
                id: point.id.clone(),
                name: point.name.clone(),
                conf: serde_json::json!(point.conf),
                value: point.value.clone(),
            });
            if resps.len() == 0 {
                break;
            }
        }

        SearchPointResp {
            total: self.points.read().await.len(),
            data: resps,
        }
    }

    pub async fn update_point(&self, point_id: Uuid, req: &CreatePointReq) -> HaliaResult<()> {
        match self.points.write().await.get_mut(&point_id) {
            Some(point) => point.update(req).await?,
            None => return Err(HaliaError::NotFound),
        };

        persistence::point::update(
            &self.device_id,
            &self.id,
            &point_id,
            &serde_json::to_string(req)?,
        )
        .await?;

        Ok(())
    }

    pub async fn get_points_num(&self) -> usize {
        self.points.read().await.len()
    }

    pub async fn delete_points(&self, ids: Vec<Uuid>) -> HaliaResult<()> {
        self.points.write().await.retain(|id, _| !ids.contains(id));
        for id in ids {
            persistence::point::delete(&self.device_id, &self.id, &id).await?;
        }
        Ok(())
    }

    pub async fn read_points_value(
        &mut self,
        ctx: &mut Context,
        interval: u64,
        rtt: &Arc<AtomicU16>,
    ) -> Result<()> {
        let mut msg = Message::default();
        for (_, point) in self.points.write().await.iter_mut() {
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

    pub async fn write(
        &self,
        ctx: &mut Context,
        interval: u64,
        point_id: Uuid,
        value: serde_json::Value,
        rtt: &Arc<AtomicU16>,
    ) {
        let now = Instant::now();
        match self.points.write().await.get_mut(&point_id) {
            Some(point) => match point.write(ctx, value).await {
                Ok(_) => rtt.store(now.elapsed().as_millis() as u16, Ordering::SeqCst),
                Err(e) => error!("write err :{}", e),
            },
            None => error!("not find point id :{}", point_id),
        }
        time::sleep(Duration::from_millis(interval)).await;
    }
}

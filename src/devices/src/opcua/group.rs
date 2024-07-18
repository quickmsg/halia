use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use opcua::{
    client::Session,
    types::{QualifiedName, ReadValueId, TimestampsToReturn, UAString},
};
use std::{sync::Arc, time::Duration};
use tokio::{
    select,
    sync::{broadcast, RwLock, RwLockWriteGuard},
    time,
};
use tracing::{debug, error};
use types::device::{
    group::{CreateGroupReq, UpdateGroupReq},
    point::{CreatePointReq, SearchPointItemResp, SearchPointResp},
};
use uuid::Uuid;

use super::point::Point;

#[derive(Clone)]
pub(crate) enum Command {
    Stop(Uuid),
    Update(Uuid, u64),
    StopAll,
}

#[derive(Debug)]
pub struct Group {
    pub id: Uuid,
    pub name: String,
    pub interval: u64,
    pub points: Arc<RwLock<Vec<Point>>>,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,
    pub desc: Option<String>,
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
            points: Arc::new(RwLock::new(vec![])),
            tx: None,
            ref_cnt: 0,
            desc: conf.desc.clone(),
        })
    }

    pub fn run(
        &self,
        session: Arc<RwLock<Option<Arc<Session>>>>,
        mut cmd_rx: broadcast::Receiver<Command>,
    ) {
        let interval = self.interval;
        let group_id = self.id;
        let points = self.points.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));
            loop {
                select! {
                    biased;
                    signal = cmd_rx.recv() => {
                        match signal {
                            Ok(cmd) => {
                                match cmd {
                                    Command::Stop(id) => if id == group_id {
                                        return
                                    }
                                    Command::StopAll => {
                                        return
                                    }
                                    Command::Update(id, duraion) => {
                                        if id == group_id {
                                            interval = time::interval(Duration::from_millis(duraion));
                                        }
                                    }
                                }
                            }
                            Err(e) => error!("group recv cmd signal err :{:?}", e),
                        }
                    }

                    _ = interval.tick() => {
                        match session.read().await.as_ref() {
                            Some(session) => {
                                Group::read_points(session, points.write().await).await;
                            }
                            None => {},
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
        let point = Point::new(point_id, req)?;
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
                conf: serde_json::json!(point.node_id),
                value: serde_json::to_value(&point.value).unwrap(),
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

    async fn read_points(session: &Session, mut points: RwLockWriteGuard<'_, Vec<Point>>) {
        debug!("read points");
        let mut nodes = vec![];
        for point in points.iter() {
            nodes.push(ReadValueId {
                node_id: point.node_id.clone(),
                attribute_id: 13,
                index_range: UAString::null(),
                data_encoding: QualifiedName::null(),
            })
        }
        match session.read(&nodes, TimestampsToReturn::Both, 2000.0).await {
            Ok(mut data_values) => {
                for point in points.iter_mut().rev() {
                    point.write(data_values.pop());
                }
            }
            Err(e) => {
                error!("{e:?}");
            }
        }
    }
}

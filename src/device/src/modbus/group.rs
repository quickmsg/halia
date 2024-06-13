use common::error::{HaliaError, HaliaResult};
use message::{Message, MessageBatch};
use protocol::modbus::client::Context;
use std::time::Duration;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tracing::{debug, error};
use types::device::{CreateGroupReq, CreatePointReq, ListPointResp, UpdateGroupReq};
use uuid::Uuid;

use super::point::Point;
use crate::storage;

#[derive(Debug)]
pub(crate) struct Group {
    pub id: Uuid,
    pub name: String,
    pub interval: u64,
    pub points: RwLock<Vec<(Uuid, Point)>>,
    pub device_id: Uuid,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    // pub subscirbers: u16,
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
    pub fn new(device_id: Uuid, group_id: Uuid, conf: &CreateGroupReq) -> Self {
        Group {
            id: group_id,
            device_id,
            name: conf.name.clone(),
            interval: conf.interval,
            points: RwLock::new(vec![]),
            tx: None,
        }
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
        match &self.tx {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel::<MessageBatch>(20);
                self.tx = Some(tx);
                rx
            }
        }
    }

    pub async fn create_points(
        &self,
        create_points: Vec<(Option<Uuid>, CreatePointReq)>,
    ) -> HaliaResult<()> {
        let mut points = Vec::with_capacity(create_points.len());
        let mut storage_infos = Vec::with_capacity(create_points.len());
        for (point_id, conf) in create_points {
            match point_id {
                Some(point_id) => {
                    let point = Point::new(conf.clone(), point_id)?;
                    points.push((point_id, point));
                }
                None => {
                    let point_id = Uuid::new_v4();
                    let point = Point::new(conf.clone(), point_id)?;
                    points.push((point_id, point));
                    storage_infos.push((point_id, serde_json::to_string(&conf)?));
                }
            }
        }

        self.points.write().await.extend(points);
        if storage_infos.len() > 0 {
            storage::insert_points(self.device_id, self.id, &storage_infos).await?;
        }
        Ok(())
    }

    pub async fn read_points(&self) -> Vec<ListPointResp> {
        let mut resps = Vec::with_capacity(self.get_points_num().await);
        for (_, point) in self.points.read().await.iter() {
            resps.push(ListPointResp {
                id: point.id,
                name: point.name.clone(),
                address: point.conf.address,
                r#type: "int16".to_string(),
                value: point.value.clone(),
                describe: point.conf.describe.clone(),
            })
        }

        resps
    }

    pub async fn update_point(&self, point_id: Uuid, req: &CreatePointReq) -> HaliaResult<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|(id, _)| *id == point_id)
        {
            Some((_, point)) => point.update(req).await?,
            None => return Err(HaliaError::NotFound),
        };

        storage::update_point(
            self.device_id,
            self.id,
            point_id,
            serde_json::to_string(req)?,
        )
        .await?;

        Ok(())
    }

    pub async fn get_points_num(&self) -> usize {
        self.points.read().await.len()
    }

    pub async fn delete_points(&self, ids: Vec<Uuid>) -> HaliaResult<()> {
        self.points
            .write()
            .await
            .retain(|(id, _)| !ids.contains(id));
        storage::delete_points(self.device_id, self.id, &ids).await?;
        Ok(())
    }

    pub async fn read(&self, ctx: &mut Context, interval: u64) {
        let mut msg = Message::new();
        for (id, point) in self.points.write().await.iter_mut() {
            match point.read(ctx).await {
                Ok(data) => msg.add(&point.name, data),
                Err(_) => todo!(),
            }
            time::sleep(Duration::from_millis(interval)).await;
        }

        if let Some(tx) = &self.tx {
            if let Err(e) = tx.send(MessageBatch::from_message(msg)) {
                error!("send message batch err :{}", e);
            }
        }
    }
}

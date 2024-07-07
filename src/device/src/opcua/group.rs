use std::time::Duration;

use anyhow::{bail, Result};
use message::MessageBatch;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tracing::{debug, error};
use types::device::group::CreateGroupReq;
use uuid::Uuid;

use super::point::Point;

#[derive(Clone)]
pub(crate) enum Command {
    Stop(Uuid),
    Pause,
    Restart,
    Update(Uuid, u64),
    StopAll,
}

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
}

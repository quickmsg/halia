use std::{sync::Arc, time::Duration};

use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use opcua::client::Session;
use tokio::{
    select,
    sync::{broadcast, RwLock},
    time,
};
use tracing::{debug, error};
use types::devices::opcua::{
    CreateUpdateGroupReq, CreateUpdateGroupVariableReq, SearchGroupVariablesResp,
};
use uuid::Uuid;

use super::group_variable::Variable;

#[derive(Clone)]
pub(crate) enum Command {
    Stop(Uuid),
    Update(Uuid, u64),
    StopAll,
}

#[derive(Debug)]
pub struct Group {
    pub id: Uuid,
    pub conf: CreateUpdateGroupReq,
    pub variables: Arc<RwLock<Vec<Variable>>>,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,
}

impl Group {
    pub fn new(
        device_id: &Uuid,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<Self> {
        let (group_id, new) = match group_id {
            Some(group_id) => (group_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {}

        Ok(Self {
            id: group_id,
            variables: Arc::new(RwLock::new(vec![])),
            tx: None,
            ref_cnt: 0,
            conf: req,
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

    pub fn update(&mut self, req: CreateUpdateGroupReq) {
        todo!()
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

    pub async fn create_variable(
        &self,
        device_id: &Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        let variable = Variable::new(device_id, &self.id, variable_id, req).await?;
        self.variables.write().await.push(variable);
        Ok(())
    }

    pub async fn search_variables(&self, page: usize, size: usize) -> SearchGroupVariablesResp {
        let mut resps = vec![];
        for variable in self
            .variables
            .read()
            .await
            .iter()
            .rev()
            .skip((page - 1) * size)
        {
            resps.push(variable.search());
            if resps.len() == size {
                break;
            }
        }

        SearchGroupVariablesResp {
            total: self.variables.read().await.len(),
            data: resps,
        }
    }

    pub async fn update_variable(
        &self,
        variable_id: Uuid,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        match self
            .variables
            .write()
            .await
            .iter_mut()
            .find(|variable| variable.id == variable_id)
        {
            Some(variable) => variable.update(req).await,
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_variables(&self, variable_ids: Vec<Uuid>) -> HaliaResult<()> {
        for variable_id in &variable_ids {
            if let Some(variable) = self
                .variables
                .write()
                .await
                .iter_mut()
                .find(|variable| variable.id == *variable_id)
            {
                variable.delete().await?;
            }
        }
        self.variables
            .write()
            .await
            .retain(|variable| !variable_ids.contains(&variable.id));

        Ok(())
    }

    // async fn read_points(session: &Session, mut points: RwLockWriteGuard<'_, Vec<Point>>) {
    //     debug!("read points");
    //     let mut nodes = vec![];
    //     for point in points.iter() {
    //         nodes.push(ReadValueId {
    //             node_id: point.node_id.clone(),
    //             attribute_id: 13,
    //             index_range: UAString::null(),
    //             data_encoding: QualifiedName::null(),
    //         })
    //     }
    //     match session.read(&nodes, TimestampsToReturn::Both, 2000.0).await {
    //         Ok(mut data_values) => {
    //             for point in points.iter_mut().rev() {
    //                 point.write(data_values.pop());
    //             }
    //         }
    //         Err(e) => {
    //             error!("{e:?}");
    //         }
    //     }
    // }
}

use std::{sync::Arc, time::Duration};

use common::{error::HaliaResult, persistence};
use opcua::{
    client::Session,
    types::{ReadValueId, TimestampsToReturn},
};
use tokio::{
    select,
    sync::{mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::debug;
use types::devices::opcua::{
    CreateUpdateGroupReq, CreateUpdateGroupVariableReq, SearchGroupVariablesResp,
    SearchGroupsItemResp,
};
use uuid::Uuid;

use super::group_variable::Variable;

pub struct Group {
    pub id: Uuid,

    conf: CreateUpdateGroupReq,
    stop_signal_tx: Option<mpsc::Sender<()>>,

    pub on: bool,

    variables: Arc<RwLock<Vec<Variable>>>,
    read_value_ids: Arc<RwLock<Vec<ReadValueId>>>,

    handle: Option<JoinHandle<(mpsc::Receiver<()>, Arc<Session>)>>,
}

impl Group {
    pub async fn new(
        device_id: &Uuid,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<Self> {
        let (group_id, new) = match group_id {
            Some(group_id) => (group_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::opcua::create_group(
                device_id,
                &group_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: group_id,
            conf: req,
            variables: Arc::new(RwLock::new(vec![])),
            read_value_ids: Arc::new(RwLock::new(vec![])),
            on: false,
            stop_signal_tx: None,
            handle: None,
        })
    }

    pub async fn search(&self) -> SearchGroupsItemResp {
        SearchGroupsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateGroupReq) -> HaliaResult<()> {
        persistence::devices::opcua::update_group(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.group_conf != req.group_conf {
            restart = true;
        }
        self.conf = req;

        if self.on && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();
            let (stop_signal_rx, client) = self.handle.take().unwrap().await.unwrap();
            self.event_loop(stop_signal_rx, client).await;
        }

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        todo!()
    }

    pub async fn start(&mut self, client: Arc<Session>) {
        if self.on {
            return;
        } else {
            self.on = true;
        }

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);
        self.event_loop(stop_signal_rx, client).await;
    }

    async fn event_loop(&mut self, mut stop_signal_rx: mpsc::Receiver<()>, client: Arc<Session>) {
        let interval = self.conf.group_conf.interval;
        let read_value_ids = self.read_value_ids.clone();
        let handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));

            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, client)
                    }

                    _ = interval.tick() => {
                        Group::read_variables_from_remote(&client, read_value_ids.read().await.as_ref()).await;
                    }
                }
            }
        });
        self.handle = Some(handle);
    }

    async fn read_variables_from_remote(
        client: &Arc<Session>,
        read_value_ids: &[ReadValueId],
        // value: &Arc<RwLock<Option<Variant>>>,
    ) {
        match client
            .read(read_value_ids, TimestampsToReturn::Both, 2000.0)
            .await
        {
            Ok(mut resp) => {
                //  *(value.write().await) = data_value.value,
                debug!("{:?}", resp);
            }
            Err(e) => {
                debug!("err code :{:?}", e);
            }
        }
    }

    pub async fn create_variable(
        &self,
        device_id: &Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        match Variable::new(device_id, &self.id, variable_id, req).await {
            Ok(variable) => {
                self.read_value_ids
                    .write()
                    .await
                    .push(variable.get_read_value_id());
                self.variables.write().await.push(variable);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn read_variables(&self, page: usize, size: usize) -> SearchGroupVariablesResp {
        let mut data = vec![];
        for variable in self
            .variables
            .read()
            .await
            .iter()
            .rev()
            .skip((page - 1) * size)
        {
            data.push(variable.search());
            if data.len() == size {
                break;
            }
        }

        SearchGroupVariablesResp {
            total: self.variables.read().await.len(),
            data,
        }
    }

    pub async fn update_variable(
        &self,
        device_id: &Uuid,
        variable_id: Uuid,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        for (i, variable) in self.variables.write().await.iter_mut().enumerate() {
            if variable.id == variable_id {
                match variable.update(device_id, &self.id, req).await {
                    Ok(restart) => {
                        if restart {
                            let new_read_value_id = variable.get_read_value_id();
                            self.read_value_ids.write().await[i] = new_read_value_id;
                        }
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(())
    }

    pub async fn delete_variable(&self, device_id: &Uuid, variable_id: Uuid) -> HaliaResult<()> {
        for (i, variable) in self.variables.write().await.iter_mut().enumerate() {
            if variable.id == variable_id {
                match variable.delete(device_id, &self.id).await {
                    Ok(_) => {
                        self.read_value_ids.write().await.remove(i);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        self.variables
            .write()
            .await
            .retain(|variable| variable.id != variable_id);

        Ok(())
    }
}

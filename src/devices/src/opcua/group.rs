use std::{str::FromStr, sync::Arc, time::Duration};

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use opcua::{
    client::Session,
    types::{ReadValueId, TimestampsToReturn},
};
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
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

    variables: Arc<RwLock<(Vec<Variable>, Vec<ReadValueId>)>>,

    handle: Option<JoinHandle<(mpsc::Receiver<()>, Arc<Session>)>>,

    ref_info: RefInfo,
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
            variables: Arc::new(RwLock::new((vec![], vec![]))),
            on: false,
            stop_signal_tx: None,
            handle: None,
            ref_info: RefInfo::new(),
        })
    }

    pub async fn recover(&self, device_id: &Uuid) -> HaliaResult<()> {
        let variable_datas =
            persistence::devices::opcua::read_group_variables(device_id, &self.id).await?;

        for variable_data in variable_datas {
            if variable_data.len() == 0 {
                continue;
            }

            let items = variable_data
                .split(persistence::DELIMITER)
                .collect::<Vec<&str>>();
            assert_eq!(items.len(), 2);

            let variable_id = Uuid::from_str(items[0]).unwrap();
            let req: CreateUpdateGroupVariableReq = serde_json::from_str(items[1])?;
            self.create_variable(device_id, Some(variable_id), req)
                .await?;
        }

        Ok(())
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

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if !self.on {
            return Ok(());
        } else {
            self.on = false;
        }
        if !self.ref_info.can_stop() {
            return Err(HaliaError::ConfErr);
        }
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;

        Ok(())
    }

    async fn event_loop(&mut self, mut stop_signal_rx: mpsc::Receiver<()>, client: Arc<Session>) {
        let interval = self.conf.group_conf.interval;
        let variables = self.variables.clone();
        let handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));

            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, client)
                    }

                    _ = interval.tick() => {
                        Group::read_variables_from_remote(&client, &variables).await;
                    }
                }
            }
        });
        self.handle = Some(handle);
    }

    async fn read_variables_from_remote(
        client: &Arc<Session>,
        variables: &Arc<RwLock<(Vec<Variable>, Vec<ReadValueId>)>>,
    ) {
        let mut lock_variables = variables.write().await;
        match client
            .read(&lock_variables.1, TimestampsToReturn::Both, 2000.0)
            .await
        {
            Ok(mut data_values) => {
                for variable in lock_variables.0.iter_mut().rev() {
                    variable.value = data_values.pop().unwrap().value;
                }
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
            Ok((variable, read_value_id)) => {
                let mut lock_variables = self.variables.write().await;
                lock_variables.0.push(variable);
                lock_variables.1.push(read_value_id);
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
            .0
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
            total: self.variables.read().await.0.len(),
            data,
        }
    }

    pub async fn update_variable(
        &self,
        device_id: &Uuid,
        variable_id: Uuid,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        let mut lock_variables = self.variables.write().await;
        for (i, variable) in lock_variables.0.iter_mut().enumerate() {
            if variable.id == variable_id {
                match variable.update(device_id, &self.id, req).await {
                    Ok(read_value_id) => match read_value_id {
                        Some(read_value_id) => lock_variables.1[i] = read_value_id,
                        None => {}
                    },
                    Err(e) => return Err(e),
                }

                return Ok(());
            }
        }

        Err(HaliaError::NotFound)
    }

    pub async fn delete_variable(&self, device_id: &Uuid, variable_id: Uuid) -> HaliaResult<()> {
        let mut lock_variables = self.variables.write().await;
        let mut pos = -1;
        for (i, variable) in lock_variables.0.iter_mut().enumerate() {
            if variable.id == variable_id {
                pos = i as i64;
                match variable.delete(device_id, &self.id).await {
                    Ok(_) => {}
                    Err(e) => return Err(e),
                }
            }
        }

        if pos != -1 {
            lock_variables.0.remove(pos as usize);
            lock_variables.1.remove(pos as usize);
        }

        Ok(())
    }

    pub async fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.add_ref(rule_id)
    }

    pub async fn subscribe(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        self.ref_info.subscribe(rule_id)
    }

    pub async fn unsubscribe(&mut self, rule_id: &Uuid) {
        self.ref_info.unsubscribe(rule_id)
    }

    pub async fn remove_info(&mut self, rule_id: &Uuid) {
        self.ref_info.remove_ref(rule_id)
    }
}

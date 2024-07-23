use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use group::Group;
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, StatusCode},
};
use serde_json::json;
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::debug;
use types::devices::{
    opcua::{
        CreateUpdateGroupReq, CreateUpdateGroupVariableReq, CreateUpdateOpcuaReq, OpcuaConf,
        SearchGroupVariablesResp, SearchGroupsResp,
    },
    SearchDevicesItemResp,
};

use uuid::Uuid;

pub const TYPE: &str = "opcua";
mod group;
mod group_variable;
pub mod manager;

struct Opcua {
    id: Uuid,
    err: Arc<AtomicBool>,
    conf: CreateUpdateOpcuaReq,

    groups: Arc<RwLock<Vec<Group>>>,

    on: bool,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    session: Arc<RwLock<Option<Arc<Session>>>>,
}

impl Opcua {
    pub async fn new(device_id: Option<Uuid>, req: CreateUpdateOpcuaReq) -> HaliaResult<Self> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::opcua::create(
                &device_id,
                TYPE,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Opcua {
            id: device_id,
            on: false,
            err: Arc::new(AtomicBool::new(false)),
            conf: req,
            session: Arc::new(RwLock::new(None)),
            stop_signal_tx: None,
            groups: Arc::new(RwLock::new(vec![])),
        })
    }

    async fn connect(
        opcua_conf: &OpcuaConf,
    ) -> HaliaResult<(Arc<Session>, JoinHandle<StatusCode>)> {
        let mut client = ClientBuilder::new()
            .application_name("test")
            .application_uri("aasda")
            .trust_server_certs(true)
            .session_retry_limit(3)
            .create_sample_keypair(true)
            .keep_alive_interval(Duration::from_millis(100))
            .client()
            .unwrap();

        let endpoint: EndpointDescription = EndpointDescription::from(opcua_conf.host.as_ref());

        let (session, event_loop) = match client
            .new_session_from_endpoint(endpoint, IdentityToken::Anonymous)
            .await
        {
            Ok((session, event_loop)) => (session, event_loop),
            Err(e) => {
                debug!("{:?}", e);
                return Err(HaliaError::IoErr);
            }
        };

        let handle = event_loop.spawn();
        session.wait_for_connection().await;
        Ok((session, handle))
    }

    async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::devices::opcua::read_groups(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }

                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);

                    let variable_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdateGroupReq = serde_json::from_str(items[1])?;
                    self.create_group(Some(variable_id), req).await?;
                }
            }
            Err(_) => todo!(),
        }

        Ok(())
    }

    fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id,
            r#type: TYPE,
            on: self.on,
            err: self.err.load(Ordering::SeqCst),
            rtt: 9999,
            conf: json!(&self.conf),
        }
    }

    async fn start(&mut self) -> HaliaResult<()> {
        if self.on {
            return Ok(());
        } else {
            self.on = true;
        }

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        self.event_loop(stop_signal_rx).await;
        Ok(())
    }

    async fn event_loop(&mut self, mut stop_signal_rx: mpsc::Receiver<()>) {
        let opcua_conf = self.conf.opcua_conf.clone();
        let global_session = self.session.clone();
        let reconnect = self.conf.opcua_conf.reconnect;
        let groups = self.groups.clone();
        tokio::spawn(async move {
            loop {
                match Opcua::connect(&opcua_conf).await {
                    Ok((session, join_handle)) => {
                        for group in groups.write().await.iter_mut() {
                            group.start(session.clone()).await;
                        }

                        *(global_session.write().await) = Some(session);
                        match join_handle.await {
                            Ok(s) => {
                                debug!("{}", s);
                            }
                            Err(e) => debug!("{}", e),
                        }
                    }
                    Err(e) => {
                        let sleep = time::sleep(Duration::from_secs(reconnect));
                        tokio::pin!(sleep);
                        select! {
                            _ = stop_signal_rx.recv() => {
                                return
                            }

                            _ = &mut sleep => {}
                        }
                        debug!("{e}");
                    }
                }
            }
        });
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        if !self.on {
            return Ok(());
        } else {
            self.on = false;
        }

        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        match self
            .session
            .read()
            .await
            .as_ref()
            .unwrap()
            .disconnect()
            .await
        {
            Ok(_) => {
                debug!("session disconnect success");
            }
            Err(e) => {
                debug!("err code is :{}", e);
            }
        }

        Ok(())
    }

    async fn update(&mut self, req: CreateUpdateOpcuaReq) -> HaliaResult<()> {
        persistence::devices::update_device_conf(&self.id, serde_json::to_string(&req).unwrap())
            .await?;

        let mut restart = false;
        if self.conf.opcua_conf != req.opcua_conf {
            restart = true;
        }
        self.conf = req;

        if restart && self.on {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();
        }

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        Ok(())
    }

    async fn create_group(
        &mut self,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match Group::new(&self.id, group_id, req).await {
            Ok(mut group) => {
                if self.on && self.session.read().await.is_some() {
                    group
                        .start(self.session.read().await.as_ref().unwrap().clone())
                        .await;
                }
                self.groups.write().await.push(group);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_groups(&self, page: usize, size: usize) -> HaliaResult<SearchGroupsResp> {
        let mut data = vec![];
        for group in self
            .groups
            .read()
            .await
            .iter()
            .rev()
            .skip((page - 1) * size)
        {
            data.push(group.search().await);
            if data.len() == size {
                break;
            }
        }

        Ok(SearchGroupsResp {
            total: self.groups.read().await.len(),
            data,
        })
    }

    async fn update_group(&self, group_id: Uuid, req: CreateUpdateGroupReq) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update(&self.id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn delete_group(&self, group_id: Uuid) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => {
                group.delete().await?;
            }
            None => return Err(HaliaError::NotFound),
        }

        self.groups
            .write()
            .await
            .retain(|group| group.id != group_id);
        Ok(())
    }

    async fn create_group_variable(
        &mut self,
        group_id: Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.create_variable(&self.id, variable_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn read_group_variables(
        &self,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupVariablesResp> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => Ok(group.read_variables(page, size).await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group_variable(
        &self,
        group_id: Uuid,
        variable_id: Uuid,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update_variable(&self.id, variable_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_group_variable(
        &self,
        group_id: Uuid,
        variable_id: Uuid,
    ) -> HaliaResult<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.delete_variable(&self.id, variable_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn subscribe(
        &mut self,
        variable_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        todo!()
    }

    async fn unsubscribe(&mut self, group_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn create_sink(&mut self, sink_id: Uuid, req: &String) -> HaliaResult<()> {
        todo!()
    }

    async fn update_sink(&mut self, sink_id: Uuid, req: &String) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn publish(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        todo!()
    }

    async fn add_subscription(&self, req: Bytes) -> HaliaResult<()> {
        // TODO
        Err(HaliaError::ParseErr)
    }
}

use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::Status,
};
use group::Group;
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, StatusCode},
};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::{debug, error};
use types::devices::{
    opcua::{
        CreateUpdateGroupReq, CreateUpdateGroupVariableReq, CreateUpdateOpcuaReq,
        SearchGroupVariablesResp, SearchGroupsResp,
    },
    SearchDevicesItemResp,
};

use uuid::Uuid;

pub const TYPE: &str = "opcua";
mod group;
mod group_variable;

struct Opcua {
    id: Uuid,
    on: Arc<AtomicBool>,
    err: Arc<AtomicBool>,
    conf: CreateUpdateOpcuaReq,
    groups: Arc<RwLock<Vec<Group>>>,
    session: Arc<RwLock<Option<Arc<Session>>>>,
    group_signal_tx: Option<broadcast::Sender<group::Command>>,

    stop_signal_tx: Option<mpsc::Sender<()>>,
}

impl Opcua {
    pub fn new(device_id: Option<Uuid>, req: CreateUpdateOpcuaReq) -> HaliaResult<Self> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {}

        Ok(Opcua {
            id: device_id,
            on: Arc::new(AtomicBool::new(false)),
            err: Arc::new(AtomicBool::new(false)),
            conf: req,
            groups: Arc::new(RwLock::new(vec![])),
            session: Arc::new(RwLock::new(None)),
            group_signal_tx: None,
            stop_signal_tx: None,
        })
    }

    async fn run(&self) {
        let conf = self.conf.clone();
        let session = self.session.clone();
        let on = self.on.clone();
        tokio::spawn(async move {
            loop {
                match Opcua::get_session(&conf).await {
                    Ok((s, handle)) => {
                        session.write().await.replace(s);
                        match handle.await {
                            Ok(status_code) => match status_code {
                                StatusCode::Good => {
                                    if !on.load(Ordering::SeqCst) {
                                        return;
                                    }
                                }
                                _ => {
                                    *session.write().await = None;
                                    // 设备关闭后会跳到这里
                                    debug!("here");
                                }
                            },
                            Err(_) => {
                                debug!("connect err :here");
                            }
                        }
                    }
                    Err(e) => {
                        // TODO select
                        error!("connect error :{e:?}");
                        time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    async fn get_session(
        conf: &CreateUpdateOpcuaReq,
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

        let endpoint: EndpointDescription = EndpointDescription::from(conf.host.as_ref());

        let (session, event_loop) = match client
            .new_session_from_endpoint(endpoint, IdentityToken::Anonymous)
            .await
        {
            Ok((session, event_loop)) => (session, event_loop),
            Err(e) => {
                // bail!("connect error {e:?}"),
                todo!()
            }
        };

        let handle = event_loop.spawn();
        session.wait_for_connection().await;
        Ok((session, handle))
    }

    fn get_id(&self) -> Uuid {
        self.id
    }

    async fn recover(&mut self, status: Status) -> HaliaResult<()> {
        todo!()
    }

    fn get_info(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id,
            r#type: TYPE,
            on: self.on.load(Ordering::SeqCst),
            err: self.err.load(Ordering::SeqCst),
            rtt: 9999,
            conf: json!(&self.conf),
        }
    }

    async fn start(&mut self) {
        if self.on.load(Ordering::SeqCst) {
            return;
        } else {
            self.on.store(true, Ordering::SeqCst);
        }
        self.run().await;
        let (group_signal_tx, _) = broadcast::channel::<group::Command>(16);
        for group in self.groups.write().await.iter_mut() {
            group.run(self.session.clone(), group_signal_tx.subscribe());
        }
        self.group_signal_tx = Some(group_signal_tx);
    }

    async fn stop(&mut self) {
        if !self.on.load(Ordering::SeqCst) {
            return;
        } else {
            self.on.store(false, Ordering::SeqCst);
        }

        match self
            .group_signal_tx
            .as_ref()
            .unwrap()
            .send(group::Command::StopAll)
        {
            Ok(_) => {}
            Err(e) => {
                error!("send stop all command err :{}", e);
            }
        }

        match self
            .session
            .write()
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
    }

    async fn update(&mut self, req: CreateUpdateOpcuaReq) -> HaliaResult<()> {
        // if self.name != req.name {
        //     self.name = req.name.clone();
        // }

        // let mut conf = self.conf.write().await;
        // if conf.url != new_conf.url {
        //     conf.url = new_conf.url;
        //     let _ = self
        //         .session
        //         .write()
        //         .await
        //         .as_ref()
        //         .unwrap()
        //         .disconnect()
        //         .await;
        // }
        // restart

        Ok(())
    }

    async fn create_group(
        &mut self,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match Group::new(&self.id, group_id, req) {
            Ok(group) => {
                if self.on.load(Ordering::SeqCst) {
                    group.run(
                        self.session.clone(),
                        self.group_signal_tx.as_ref().unwrap().subscribe(),
                    );
                }

                self.groups.write().await.push(group);

                Ok(())
            }
            Err(_) => todo!(),
        }
    }

    async fn search_groups(&self, page: usize, size: usize) -> HaliaResult<SearchGroupsResp> {
        let mut resps = Vec::new();
        for group in self
            .groups
            .read()
            .await
            .iter()
            .rev()
            .skip(((page - 1) * size) as usize)
        {
            resps.push(group.search());
            if resps.len() == size as usize {
                break;
            }
        }
        Ok(SearchGroupsResp {
            total: self.groups.read().await.len(),
            data: resps,
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
            Some(group) => group.update(req),
            None => return Err(HaliaError::NotFound),
        };

        Ok(())
    }

    async fn delete_group(&self, group_id: Uuid) -> HaliaResult<()> {
        self.groups
            .write()
            .await
            .retain(|group| group_id != group.id);

        if self.on.load(Ordering::SeqCst) {
            match self
                .group_signal_tx
                .as_ref()
                .unwrap()
                .send(group::Command::Stop(group_id))
            {
                Ok(_) => {}
                Err(e) => error!("group send stop singla err:{}", e),
            }
        }

        Ok(())
    }

    async fn create_group_variable(
        &self,
        group_id: Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.create_variable(&self.id, variable_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn search_group_variables(
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
            Some(group) => Ok(group.search_variables(page, size).await),
            None => Err(HaliaError::NotFound),
        }
    }

    async fn update_group_variable(
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
            Some(group) => group.update_variable(variable_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn write_point_value(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        value: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
        // match self
        //     .groups
        //     .read()
        //     .await
        //     .iter()
        //     .find(|group| group.id == group_id)
        // {
        //     Some(group) => group.update_point(point_id, req).await,
        //     None => {
        //         debug!("未找到组");
        //         Err(HaliaError::NotFound)
        //     }
        // }
    }

    async fn delete_group_variables(
        &self,
        group_id: &Uuid,
        variable_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => group.delete_variables(variable_ids).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn subscribe(
        &mut self,
        group_id: &Uuid,
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

use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
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
    opcua::{CreateUpdateOpcuaReq, CreateUpdateVariableReq, SearchVariablesResp},
    SearchDevicesItemResp,
};
use variable::Variable;

use uuid::Uuid;

pub const TYPE: &str = "opcua";
pub mod manager;
mod variable;

struct Opcua {
    id: Uuid,
    on: Arc<AtomicBool>,
    err: Arc<AtomicBool>,
    conf: CreateUpdateOpcuaReq,

    variables: Vec<Variable>,

    session: Arc<RwLock<Option<Arc<Session>>>>,

    stop_signal_tx: Option<mpsc::Sender<()>>,
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
            on: Arc::new(AtomicBool::new(false)),
            err: Arc::new(AtomicBool::new(false)),
            conf: req,
            session: Arc::new(RwLock::new(None)),
            stop_signal_tx: None,
            variables: vec![],
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

    async fn recover(&mut self) -> HaliaResult<()> {
        todo!()
    }

    fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id,
            r#type: TYPE,
            on: self.on.load(Ordering::SeqCst),
            err: self.err.load(Ordering::SeqCst),
            rtt: 9999,
            conf: json!(&self.conf),
        }
    }

    async fn start(&mut self) -> HaliaResult<()> {
        if self.on.load(Ordering::SeqCst) {
            return Ok(());
        } else {
            self.on.store(true, Ordering::SeqCst);
        }
        self.run().await;
        todo!()
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        if !self.on.load(Ordering::SeqCst) {
            return Ok(());
        } else {
            self.on.store(false, Ordering::SeqCst);
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

        Ok(())
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

    pub async fn delete(&mut self) -> HaliaResult<()> {
        Ok(())
    }

    async fn create_variable(
        &mut self,
        variable_id: Option<Uuid>,
        req: CreateUpdateVariableReq,
    ) -> HaliaResult<()> {
        // match self
        //     .groups
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|group| group.id == group_id)
        // {
        //     Some(group) => group.create_variable(&self.id, variable_id, req).await,
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    async fn search_variables(&self, page: usize, size: usize) -> HaliaResult<SearchVariablesResp> {
        // match self
        //     .groups
        //     .read()
        //     .await
        //     .iter()
        //     .find(|group| group.id == group_id)
        // {
        //     Some(group) => Ok(group.search_variables(page, size).await),
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    async fn update_variable(
        &mut self,
        variable_id: Uuid,
        req: CreateUpdateVariableReq,
    ) -> HaliaResult<()> {
        // match self
        //     .groups
        //     .read()
        //     .await
        //     .iter()
        //     .find(|group| group.id == group_id)
        // {
        //     Some(group) => group.update_variable(variable_id, req).await,
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    async fn delete_variable(&mut self, variable_id: Uuid) -> HaliaResult<()> {
        // match self
        //     .groups
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|group| group.id == *group_id)
        // {
        //     Some(group) => group.delete_variables(variable_ids).await,
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
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

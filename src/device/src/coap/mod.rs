use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, StatusCode},
};
use serde::{Deserialize, Serialize};
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
use types::device::device::{
    CreateDeviceReq, SearchDeviceItemResp, SearchSinksResp, UpdateDeviceReq,
};
use uuid::Uuid;

use crate::Device;

pub(crate) const TYPE: &str = "coap";
mod path;

struct OpcUa {
    id: Uuid,
    name: String,
    on: Arc<AtomicBool>,
    err: Arc<AtomicBool>,
    conf: Arc<RwLock<Conf>>,
    session: Arc<RwLock<Option<Arc<Session>>>>,
}

#[derive(Deserialize, Serialize, Clone)]
struct Conf {
    url: String,
}

pub(crate) fn new(id: Uuid, req: &CreateDeviceReq) -> HaliaResult<Box<dyn Device>> {
    let conf: Conf = serde_json::from_value(req.conf.clone())?;
    Ok(Box::new(OpcUa {
        id,
        name: req.name.clone(),
        on: Arc::new(AtomicBool::new(false)),
        err: Arc::new(AtomicBool::new(false)),
        conf: Arc::new(RwLock::new(conf)),
        session: Arc::new(RwLock::new(None)),
    }))
}

impl OpcUa {
    async fn run(&self) {
        let conf = self.conf.clone();
        let session = self.session.clone();
        let on = self.on.clone();
        tokio::spawn(async move {
            let now_conf = conf.read().await;

            loop {
                match OpcUa::get_session(&now_conf).await {
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

    async fn get_session(conf: &Conf) -> Result<(Arc<Session>, JoinHandle<StatusCode>)> {
        let mut client = ClientBuilder::new()
            .application_name("test")
            .application_uri("aasda")
            .trust_server_certs(true)
            .session_retry_limit(3)
            .create_sample_keypair(true)
            .keep_alive_interval(Duration::from_millis(100))
            .client()
            .unwrap();

        let endpoint: EndpointDescription = EndpointDescription::from(conf.url.as_ref());

        let (session, event_loop) = match client
            .new_session_from_endpoint(endpoint, IdentityToken::Anonymous)
            .await
        {
            Ok((session, event_loop)) => (session, event_loop),
            Err(e) => bail!("connect error {e:?}"),
        };

        let handle = event_loop.spawn();
        session.wait_for_connection().await;
        Ok((session, handle))
    }
}

#[async_trait]
impl Device for OpcUa {
    fn get_id(&self) -> Uuid {
        self.id
    }

    async fn get_info(&self) -> SearchDeviceItemResp {
        SearchDeviceItemResp {
            id: self.id,
            name: self.name.clone(),
            r#type: TYPE,
            on: self.on.load(Ordering::SeqCst),
            err: self.err.load(Ordering::SeqCst),
            rtt: 9999,
            conf: json!(&self.conf.read().await.clone()),
        }
    }

    async fn start(&mut self) -> HaliaResult<()> {
        if self.on.load(Ordering::SeqCst) {
            return Ok(());
        } else {
            self.on.store(true, Ordering::SeqCst);
        }
        self.run().await;

        Ok(())
    }

    async fn stop(&mut self) {
        if !self.on.load(Ordering::SeqCst) {
            return;
        } else {
            self.on.store(false, Ordering::SeqCst);
        }
    }

    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()> {
        let new_conf: Conf = serde_json::from_value(req.conf.clone())?;
        if self.name != req.name {
            self.name = req.name.clone();
        }

        let mut conf = self.conf.write().await;
        if conf.url != new_conf.url {
            conf.url = new_conf.url;
            let _ = self
                .session
                .write()
                .await
                .as_ref()
                .unwrap()
                .disconnect()
                .await;
        }

        Ok(())
    }

    async fn add_path(&self, _req: Bytes) -> HaliaResult<()> {
        Err(HaliaError::ProtocolNotSupported)
    }
    async fn search_paths(&self, _page: usize, _size: usize) -> HaliaResult<()> {
        Err(HaliaError::ProtocolNotSupported)
    }
    async fn update_path(&self, _req: Bytes) -> HaliaResult<()> {
        Err(HaliaError::ProtocolNotSupported)
    }
    async fn delete_path(&self, _req: Bytes) -> HaliaResult<()> {
        Err(HaliaError::ProtocolNotSupported)
    }

    async fn subscribe(&mut self, id: &Uuid) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        todo!()
    }

    async fn unsubscribe(&mut self, id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn create_sink(&self, sink_id: Uuid, req: &Bytes) -> HaliaResult<()> {
        todo!()
    }

    async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp {
        todo!()
    }

    async fn update_sink(&self, sink_id: Uuid, req: &Bytes) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_sink(&self, sink_id: Uuid) -> HaliaResult<()> {
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

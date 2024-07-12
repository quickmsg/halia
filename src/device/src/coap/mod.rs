use anyhow::{bail, Result};
use async_trait::async_trait;
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
use path::Path;
use protocol::coap::client::{CoAPClient, UdpCoAPClient, UdpTransport};
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
use types::{
    device::device::{CreateDeviceReq, SearchDeviceItemResp, SearchSinksResp, UpdateDeviceReq},
    SearchResp,
};
use uuid::Uuid;

use crate::Device;

pub(crate) const TYPE: &str = "coap";
mod path;

struct Coap {
    id: Uuid,
    name: String,
    on: Arc<AtomicBool>,
    err: Arc<AtomicBool>,
    conf: Arc<RwLock<Conf>>,
    client: Arc<RwLock<Option<UdpCoAPClient>>>,
    paths: RwLock<Vec<Path>>,
}

#[derive(Deserialize, Serialize, Clone)]
struct Conf {
    host: String,
    port: u16,
}

pub async fn new(id: Uuid, req: CreateDeviceReq) -> HaliaResult<Box<dyn Device>> {
    let conf: Conf = serde_json::from_value(req.conf.clone())?;
    Ok(Box::new(Coap {
        id,
        name: req.name.clone(),
        on: Arc::new(AtomicBool::new(false)),
        err: Arc::new(AtomicBool::new(false)),
        conf: Arc::new(RwLock::new(conf)),
        client: Arc::new(RwLock::new(None)),
        paths: RwLock::new(vec![]),
    }))
}

impl Coap {
    async fn run(&self) {
        let conf = self.conf.clone();
        let on = self.on.clone();
        let client =
            UdpCoAPClient::new_udp((conf.read().await.host.clone(), conf.read().await.port)).await;

        match client {
            Ok(client) => *self.client.write().await = Some(client),
            Err(e) => {
                error!("create client err :{}", e);
            }
        }
    }

    // async fn get_session(conf: &Conf) -> Result<(Arc<Session>, JoinHandle<StatusCode>)> {
    //     let mut client = ClientBuilder::new()
    //         .application_name("test")
    //         .application_uri("aasda")
    //         .trust_server_certs(true)
    //         .session_retry_limit(3)
    //         .create_sample_keypair(true)
    //         .keep_alive_interval(Duration::from_millis(100))
    //         .client()
    //         .unwrap();

    //     let endpoint: EndpointDescription = EndpointDescription::from(conf.url.as_ref());

    //     let (session, event_loop) = match client
    //         .new_session_from_endpoint(endpoint, IdentityToken::Anonymous)
    //         .await
    //     {
    //         Ok((session, event_loop)) => (session, event_loop),
    //         Err(e) => bail!("connect error {e:?}"),
    //     };

    //     let handle = event_loop.spawn();
    //     session.wait_for_connection().await;
    //     Ok((session, handle))
    // }
}

#[async_trait]
impl Device for Coap {
    fn get_id(&self) -> Uuid {
        self.id
    }

    async fn recover(&mut self, status: Status) -> HaliaResult<()> {
        let paths = persistence::device::read_coap_paths(&self.id).await?;
        for (id, data) in paths {
            let path = path::new(id, &data).await?;
            self.paths.write().await.push(path);
        }
        Ok(())
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
        Ok(())
    }

    async fn add_path(&mut self, id: Uuid, req: String) -> HaliaResult<()> {
        let mut path = path::new(id, &req).await?;
        if self.on.load(Ordering::SeqCst) {
            path.start(self.client.clone()).await;
        }
        self.paths.write().await.push(path);
        Ok(())
    }

    async fn search_paths(&self, page: usize, size: usize) -> HaliaResult<SearchResp> {
        let mut data = vec![];
        let mut i = 0;
        for path in self.paths.read().await.iter().skip((page - 1) * size) {
            data.push(json!(path.id));
            i += 1;
            if i == size {
                break;
            }
        }
        Ok(SearchResp {
            total: self.paths.read().await.len(),
            data,
        })
    }

    async fn update_path(&self, path_id: Uuid, req: Bytes) -> HaliaResult<()> {
        match self
            .paths
            .write()
            .await
            .iter_mut()
            .find(|path| path.id == path_id)
        {
            Some(path) => match path.update(req).await {
                Ok(restart) => {
                    if self.on.load(Ordering::SeqCst) && !self.err.load(Ordering::SeqCst) && restart
                    {
                        path.stop().await;
                        path.start(self.client.clone()).await;
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            },
            None => Err(HaliaError::NotFound),
        }
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

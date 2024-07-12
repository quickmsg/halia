use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use common::error::HaliaResult;
use protocol::coap::{
    client::UdpCoAPClient,
    request::{Method, RequestBuilder},
};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
    time,
};
use tracing::debug;
use uuid::Uuid;

pub struct Path {
    pub id: Uuid,
    conf: Conf,
    stop_signal_tx: Option<mpsc::Sender<()>>,
}

// TODO validate
#[derive(Deserialize, Serialize)]
struct Conf {
    name: String,
    path: String,
    queries: Option<String>,
    observe: bool,
    request: Option<Request>,
}

#[derive(Deserialize, Serialize, PartialEq)]
struct Request {
    timeout: Option<usize>,
    interval: u64,
}

pub fn new(id: Uuid, conf: Bytes) -> HaliaResult<Path> {
    let conf: Conf = serde_json::from_slice(&conf)?;
    Ok(Path {
        id,
        conf,
        stop_signal_tx: None,
    })
}

impl Path {
    pub async fn start(&mut self, client: Arc<RwLock<Option<UdpCoAPClient>>>) {
        let (tx, mut rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(tx);
        if self.conf.observe {
            todo!()
        } else {
            let interval = self.conf.request.as_ref().unwrap().interval;
            let path = self.conf.path.clone();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(interval));

                loop {
                    select! {
                        biased;
                        _ = rx.recv() => {
                            debug!("path is stop");
                            return
                        }
                        _ = interval.tick() => {
                            let request = RequestBuilder::new(&path, Method::Get)
                                .queries(None)
                                .domain("127.0.0.1".to_string())
                                .build();
                            match client.read().await.as_ref().unwrap().send(request).await {
                                Ok(resp) => debug!("{resp:?}"),
                                Err(e) => debug!("{e:?}"),
                            }
                        }
                    }
                }
            });
        }
    }

    pub async fn stop(&mut self) {
        match self.stop_signal_tx.as_ref().unwrap().send(()).await {
            Ok(()) => debug!("send stop signal ok"),
            Err(e) => debug!("send stop signal err :{e:?}"),
        }
        self.stop_signal_tx = None;
    }

    // 是否需要重启
    pub async fn update(&mut self, update_conf: Bytes) -> HaliaResult<bool> {
        let update_conf: Conf = serde_json::from_slice(&update_conf)?;
        let mut restart = false;
        if update_conf.path != self.conf.path
            || update_conf.queries != self.conf.queries
            || update_conf.observe != self.conf.observe
            || update_conf.request != self.conf.request
        {
            restart = true;
        }
        self.conf = update_conf;
        Ok(restart)
    }
}

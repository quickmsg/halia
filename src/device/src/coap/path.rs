use std::{sync::Arc, time::Duration};

use anyhow::Result;
use bytes::Bytes;
use common::error::HaliaResult;
use protocol::coap::{
    client::{CoAPClient, UdpCoAPClient},
    request::{CoapRequest, Method, RequestBuilder},
};
use serde::{Deserialize, Serialize};
use tokio::{select, sync::RwLock, time};
use tracing::debug;
use uuid::Uuid;

pub struct Path {
    id: Uuid,
    conf: Arc<RwLock<Conf>>,
}

#[derive(Deserialize, Serialize)]
struct Conf {
    path: String,
    queries: Option<String>,
    observe: bool,
    request: Option<Request>,
}

#[derive(Deserialize, Serialize)]
struct Request {
    timeout: Option<usize>,
    interval: u64,
}

pub fn new(id: Uuid, conf: Bytes) -> HaliaResult<Path> {
    let conf: Conf = serde_json::from_slice(&conf)?;
    Ok(Path {
        id,
        conf: Arc::new(RwLock::new(conf)),
    })
}

impl Path {
    pub async fn run(&self, client: Arc<RwLock<Option<UdpCoAPClient>>>) {
        if self.conf.read().await.observe {
            todo!()
        } else {
            let interval = self.conf.read().await.request.as_ref().unwrap().interval;
            let path = self.conf.read().await.path.clone();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(interval));

                loop {
                    select! {
                        _ = interval.tick() => {
                            let request = RequestBuilder::new(&path, Method::Get)
                                .queries(None)
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
}

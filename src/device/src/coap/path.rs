use std::{sync::Arc, time::Duration};

use anyhow::Result;
use protocol::coap::{
    client::{CoAPClient, UdpCoAPClient},
    request::{CoapRequest, Method, RequestBuilder},
};
use tokio::{select, sync::RwLock, time};
use tracing::debug;
use uuid::Uuid;

struct Path {
    id: Uuid,
    conf: Arc<Conf>,
}

struct Conf {
    path: String,
    queries: Option<String>,
    observe: bool,
    request: Option<Request>,
}

struct Request {
    timeout: Option<usize>,
    interval: u64,
}

fn new(id: Uuid, conf: serde_json::Value) -> Result<Path> {
    todo!()
}

impl Path {
    pub fn run(&self, client: Arc<UdpCoAPClient>) {
        if self.conf.observe {
            todo!()
        } else {
            let interval = self.conf.request.as_ref().unwrap().interval;
            let path = self.conf.path.clone();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(interval));

                loop {
                    select! {
                        _ = interval.tick() => {
                            let request = RequestBuilder::new(&path, Method::Get)
                            .queries(None)
                            .build();
                            match client.send(request).await {
                                Ok(resp) => debug!("{resp:?}"),
                                Err(_) => todo!(),
                            }
                        }
                    }
                }
            });
        }
    }
}

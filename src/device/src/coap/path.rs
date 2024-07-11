use std::{sync::Arc, time::Duration};

use anyhow::Result;
use protocol::coap::{client::UdpCoAPClient, request::CoapRequest};
use serde::{Deserialize, Serialize};
use tokio::{select, time};
use uuid::Uuid;

struct Path {
    id: Uuid,
    conf: Conf,
}

struct Conf {
    path: String,
    r#type: String,
    request: Option<Request>,
    observe: Option<Observe>,
}

struct Request {
    method: Method,
    data: Option<Vec<u8>>,
    timeout: Option<usize>,
    interval: u64,
    // request: CoapRequest,
}

struct Observe {}

#[derive(Deserialize, Serialize)]
enum Method {
    GET,
    POST,
    PUT,
    DELETE,
}

fn new(id: Uuid, conf: serde_json::Value) -> Result<Path> {
    todo!()
}

impl Path {
    pub fn run(&self, client: Arc<UdpCoAPClient>) {
        if let Some(request) = &self.conf.request {
            let interval = request.interval;
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(interval));
                loop {
                    select! {
                        _ = interval.tick() => {
                            // TODO
                        //     match client.send(request).await {
                        //         Ok(_) => todo!(),
                        //         Err(_) => todo!(),
                        //     }
                        }
                    }
                }
            });
        }
    }
}

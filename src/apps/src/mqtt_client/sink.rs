use anyhow::Result;
use message::MessageBatch;
use rumqttc::{AsyncClient, QoS};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: Conf,
    pub tx: Option<mpsc::Receiver<MessageBatch>>,
    pub ref_cnt: u8,
    pub client: Option<AsyncClient>,
}

#[derive(Deserialize, Serialize)]
struct Conf {
    topic: String,
    qos: u8,
}

impl Sink {
    pub fn new(app_id: &Uuid, sink_id: Option<Uuid>, data: String) -> Result<Self> {
        todo!()
    }

    pub fn start(&self, mut rx: mpsc::Receiver<MessageBatch>) {
        let client = self.client.as_ref().unwrap().clone();
        let topic = self.conf.topic.clone();
        let qos = self.conf.qos;
        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => unreachable!(),
        };

        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(mb) => match client.publish(&topic, qos, false, mb.to_json()).await {
                        Ok(_) => debug!("publish msg success"),
                        Err(e) => error!("publish msg err:{e:?}"),
                    },
                    None => return,
                }
            }
        });
    }

    pub fn stop() {}

    pub fn search(&self) {}

    pub fn update(&mut self, data: String) {}

    pub fn delete(&mut self) {}
}

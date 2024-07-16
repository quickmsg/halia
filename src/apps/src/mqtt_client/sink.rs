use common::error::HaliaResult;
use message::MessageBatch;
use rumqttc::{AsyncClient, QoS};
use tokio::sync::mpsc;
use tracing::{debug, error};
use types::apps::mqtt_client::{CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
    pub tx: Option<mpsc::Receiver<MessageBatch>>,
    pub ref_cnt: u8,
    pub client: Option<AsyncClient>,
}

impl Sink {
    pub async fn new(
        app_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<Self> {
        let (sink_id, new) = match sink_id {
            Some(sink_id) => (sink_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            // 持久化
        }

        Ok(Sink {
            id: sink_id,
            conf: req,
            tx: None,
            ref_cnt: 0,
            client: None,
        })
    }

    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub fn update(&mut self, data: String) {}

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

    pub fn delete(&mut self) {}
}

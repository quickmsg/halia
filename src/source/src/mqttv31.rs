use anyhow::Result;
use common::error::{HaliaError, HaliaResult};
use common::persistence;
use message::MessageBatch;
use rumqttc::v5::mqttbytes::{qos, QoS};
use rumqttc::v5::{AsyncClient, ConnectionError, Event, Incoming, MqttOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::string::String;
use std::sync::Arc;
use tokio;
use tokio::sync::broadcast::{self, Receiver};
use tokio::sync::RwLock;
use tokio::time::Duration;
use tracing::{debug, error};
use types::source::mqtt::{SearchTopicResp, TopicReq, TopicResp};
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

use crate::mqtt_topic::matches;

pub(crate) static TYPE: &str = "mqtt";

pub struct Mqtt {
    id: Uuid,
    name: String,
    conf: Conf,
    client: Option<AsyncClient>,
    status: bool,
    topics: Arc<RwLock<Vec<Topic>>>,
}

struct Topic {
    pub id: Uuid,
    pub topic: String,
    pub qos: u8,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Conf {
    id: String,
    host: String,
    port: u16,
}

impl Mqtt {
    pub fn new(id: Uuid, req: &CreateSourceReq) -> HaliaResult<Self> {
        let conf: Conf = serde_json::from_value(req.conf.clone())?;
        Ok(Mqtt {
            client: None,
            id,
            name: req.name.clone(),
            conf,
            status: false,
            topics: Arc::new(RwLock::new(vec![])),
        })
    }

    async fn run(&mut self) {
        let mut mqtt_options =
            MqttOptions::new(self.conf.id.clone(), self.conf.host.clone(), self.conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10);
        self.client = Some(client);

        let topics = self.topics.clone();
        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        let topic_str = std::str::from_utf8(&p.topic).unwrap();
                        match MessageBatch::from_json(&p.payload) {
                            Ok(msg) => {
                                for topic in topics.write().await.iter_mut() {
                                    if matches(&topic.topic, topic_str) {
                                        let _ = topic.tx.as_ref().unwrap().send(msg.clone());
                                    }
                                }
                            }
                            Err(e) => error!("Failed to decode msg:{}", e),
                        }
                    }
                    Ok(_) => (),
                    Err(e) => {
                        if let ConnectionError::Timeout(_) = e {
                            continue;
                        }
                        // match client.subscribe(conf.topic.clone(), QoS::AtMostOnce).await {
                        //     Ok(_) => {}
                        //     Err(e) => error!("Failed to connect mqtt server:{}", e),
                        // }
                    }
                }
            }
        });
    }

    pub async fn subscribe(&mut self, id: Uuid) -> HaliaResult<Receiver<MessageBatch>> {
        if self.client.is_none() {
            self.run().await;
        }

        match self
            .topics
            .write()
            .await
            .iter_mut()
            .find(|topic| topic.id == id)
        {
            Some(topic) => {
                match self
                    .client
                    .as_ref()
                    .unwrap()
                    .subscribe(topic.topic.clone(), qos(topic.qos).unwrap())
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!("{e}");
                    }
                }

                match &topic.tx {
                    Some(tx) => Ok(tx.subscribe()),
                    None => {
                        let (tx, rx) = broadcast::channel(10);
                        topic.tx = Some(tx);
                        Ok(rx)
                    }
                }
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub fn get_info(&self) -> Result<ListSourceResp> {
        Ok(ListSourceResp {
            id: self.id.clone(),
            name: self.name.clone(),
            r#type: &TYPE,
        })
    }

    pub fn get_detail(&self) -> HaliaResult<SourceDetailResp> {
        Ok(SourceDetailResp {
            id: self.id.clone(),
            r#type: "mqtt",
            name: self.name.clone(),
            conf: serde_json::json!(self.conf),
        })
    }

    fn update(&mut self, conf: Value) -> HaliaResult<()> {
        todo!()
    }

    pub async fn create_topic(&mut self, topic_id: Option<Uuid>, req: TopicReq) -> HaliaResult<()> {
        let (topic_id, create) = match topic_id {
            Some(id) => (id, false),
            None => (Uuid::new_v4(), true),
        };
        let topic = Topic {
            id: topic_id.clone(),
            topic: req.topic.clone(),
            qos: req.qos,
            tx: None,
            ref_cnt: 0,
        };
        self.topics.write().await.push(topic);

        if create {
            if let Err(e) = persistence::source_item::insert(
                self.id,
                topic_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await
            {
                error!("insert source_item err:{e:?}");
            }
        }

        if !self.client.is_none() {
            match &self.client {
                Some(client) => {
                    let qos = match req.qos {
                        0 => QoS::AtMostOnce,
                        1 => QoS::AtLeastOnce,
                        2 => QoS::ExactlyOnce,
                        _ => unreachable!(),
                    };
                    match client.subscribe(req.topic, qos).await {
                        Ok(_) => {
                            debug!("ok");
                        }
                        Err(_) => {
                            debug!("err");
                        }
                    }
                }
                None => return Err(HaliaError::NotFound),
            }
        }
        Ok(())
    }

    pub async fn search_topic(&mut self, page: usize, size: usize) -> HaliaResult<SearchTopicResp> {
        let mut topics = vec![];
        for (index, topic) in self.topics.read().await.iter().enumerate() {
            if index >= (page - 1) * size && index < page * size {
                topics.push(TopicResp {
                    id: topic.id.clone(),
                    topic: topic.topic.clone(),
                    qos: topic.qos,
                });
            }
        }

        Ok(SearchTopicResp {
            total: self.topics.read().await.len(),
            data: topics,
        })
    }

    pub async fn update_topic(&mut self, topic_id: Uuid, req: TopicReq) -> HaliaResult<()> {
        match self
            .topics
            .write()
            .await
            .iter_mut()
            .find(|topic| topic.id == topic_id)
        {
            Some(topic) => {
                topic.topic = req.topic;
                topic.qos = req.qos;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_topic(&mut self, topic_id: Uuid) -> HaliaResult<()> {
        self.topics
            .write()
            .await
            .retain(|topic| topic.id == topic_id);
        Ok(())
    }
}

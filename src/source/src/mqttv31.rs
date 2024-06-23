use anyhow::Result;
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, ConnectionError, Event, Incoming, MqttOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::string::String;
use tokio;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio::time::Duration;
use tracing::{debug, error};
use types::rule::Status;
use types::source::mqtt::{SearchTopicResp, TopicReq, TopicResp};
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

pub(crate) static TYPE: &str = "mqtt";

pub struct Mqtt {
    id: Uuid,
    name: String,
    conf: Conf,
    client: Option<AsyncClient>,
    status: Status,
    tx: Option<Sender<MessageBatch>>,
    topics: HashMap<Uuid, Topic>,
}

struct Topic {
    pub id: Uuid,
    pub topic: String,
    pub qos: u8,
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
            status: Status::Stopped,
            tx: None,
            topics: HashMap::new(),
        })
    }

    async fn run(&mut self) {
        let mut mqtt_options =
            MqttOptions::new(self.conf.id.clone(), self.conf.host.clone(), self.conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10);
        self.client = Some(client);

        let tx = match &self.tx {
            Some(tx) => tx.clone(),
            None => panic!("sendoer is none"),
        };

        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        debug!("{p:?}");
                        match MessageBatch::from_json(&p.payload) {
                            Ok(msg) => match tx.send(msg) {
                                Err(e) => error!("send message err:{}", e),
                                _ => {}
                            },
                            Err(e) => error!("Failed to decode msg:{}", e),
                        }
                    }
                    Ok(_) => (),
                    Err(e) => {
                        if let ConnectionError::Timeout(_) = e {
                            continue;
                        }
                        error!("Failed to poll mqtt eventloop:{}", e);
                        // match client.subscribe(conf.topic.clone(), QoS::AtMostOnce).await {
                        //     Ok(_) => {}
                        //     Err(e) => error!("Failed to connect mqtt server:{}", e),
                        // }
                    }
                }
            }
        });
    }

    async fn subscribe(&mut self, id: Uuid) -> HaliaResult<Receiver<MessageBatch>> {
        // let id to topic
        match self.status {
            Status::Running => match &self.tx {
                Some(tx) => Ok(tx.subscribe()),
                None => return Err(HaliaError::IoErr),
            },
            Status::Stopped => {
                let (tx, rx) = broadcast::channel(10);
                self.tx = Some(tx);
                self.run().await;
                Ok(rx)
            }
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
        let topic_id = match topic_id {
            Some(id) => id,
            None => Uuid::new_v4(),
        };

        let topic = Topic {
            id: topic_id.clone(),
            topic: req.topic.clone(),
            qos: req.qos,
        };
        self.topics.insert(topic_id, topic);

        // TODO
        match &self.client {
            Some(client) => {
                let qos = match req.qos {
                    0 => QoS::AtMostOnce,
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
            None => todo!(),
        }
        Ok(())
    }

    pub fn search_topic(&mut self, page: usize, size: usize) -> HaliaResult<SearchTopicResp> {
        let mut total = 0;

        let mut i = 0;
        let mut topics = vec![];
        for (_, topic) in &self.topics {
            total += 1;
            if i >= (page - 1) * size && i < page * size {
                topics.push(TopicResp {
                    id: topic.id.clone(),
                    topic: topic.topic.clone(),
                    qos: topic.qos,
                });
            }
            i += 1;
        }

        Ok(SearchTopicResp {
            total: total,
            data: topics,
        })
    }

    pub fn update_topic(&mut self, topic_id: Uuid, req: TopicReq) -> HaliaResult<()> {
        match self.topics.get_mut(&topic_id) {
            Some(topic) => {
                // TODO 判断是否需要更改
                topic.topic = req.topic;
                topic.qos = req.qos;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub fn delete_topic(&mut self, topic_id: Uuid) -> HaliaResult<()> {
        match self.topics.remove(&topic_id) {
            Some(_) => Ok(()),
            None => Err(HaliaError::NotFound),
        }
    }
}

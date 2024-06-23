use anyhow::Result;
use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, ConnectionError, Event, Incoming, MqttOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::string::String;
use tokio;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time::Duration;
use tracing::error;
use types::rule::Status;
use types::source::mqtt::{SearchTopicResp, TopicReq, TopicResp};
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

pub(crate) static TYPE: &str = "mqtt";

pub struct Mqtt {
    id: Uuid,
    name: String,
    conf: Conf,
    status: Status,
    tx: Option<Sender<MessageBatch>>,
    topics: HashMap<Uuid, Path>,
}

struct Path {
    pub id: Uuid,
    pub path: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Conf {
    host: String,
    port: u16,
}

impl Mqtt {
    pub fn new(id: Uuid, req: &CreateSourceReq) -> HaliaResult<Self> {
        let conf: Conf = serde_json::from_value(req.conf.clone())?;
        Ok(Mqtt {
            id,
            name: req.name.clone(),
            conf,
            status: Status::Stopped,
            tx: None,
            topics: HashMap::new(),
        })
    }

    async fn run(&self) {
    }

    async fn subscribe(&mut self) -> HaliaResult<Receiver<MessageBatch>> {
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

    fn get_type(&self) -> &'static str {
        return &TYPE;
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

    fn stop(&self) {}

    fn update(&mut self, conf: Value) -> HaliaResult<()> {
        todo!()
    }

    pub fn create_path(&mut self, topic_id: Option<Uuid>, req: TopicReq) -> HaliaResult<()> {
        todo!()
    }

    pub fn search_path(&mut self, page: usize, size: usize) -> HaliaResult<SearchTopicResp> {
        todo!()
        // let mut total = 0;

        // let mut i = 0;
        // let mut topics = vec![];
        // for (_, topic) in &self.topics {
        //     total += 1;
        //     if i >= (page - 1) * size && i < page * size {
        //         topics.push(TopicResp {
        //             id: topic.id.clone(),
        //             topic: topic.topic.clone(),
        //             qos: topic.qos,
        //         });
        //     }
        //     i += 1;
        // }

        // Ok(SearchTopicResp {
        //     total: total,
        //     data: topics,
        // })
    }

    pub fn update_path(&mut self, topic_id: Uuid, req: TopicReq) -> HaliaResult<()> {
        // match self.topics.get_mut(&topic_id) {
        //     Some(topic) => {
        //         // TODO 判断是否需要更改
        //         topic.topic = req.topic;
        //         topic.qos = req.qos;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub fn delete_path(&mut self, topic_id: Uuid) -> HaliaResult<()> {
        match self.topics.remove(&topic_id) {
            Some(_) => Ok(()),
            None => Err(HaliaError::NotFound),
        }
    }
}

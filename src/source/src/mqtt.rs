use anyhow::Result;
use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, ConnectionError, Event, Incoming, MqttOptions};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::string::String;
use tokio;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::time::Duration;
use tracing::error;
use types::rule::Status;
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

use crate::Source;

pub struct Mqtt {
    id: Uuid,
    name: String,
    conf: Conf,
    status: Status,
    tx: Option<Sender<MessageBatch>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Conf {
    id: String,
    host: String,
    topic: String,
    port: u16,
}

impl Mqtt {
    pub fn new(id: Uuid, req: &CreateSourceReq) -> HaliaResult<Box<dyn Source + Sync + Send>> {
        let conf: Conf = serde_json::from_value(req.conf.clone())?;
        Ok(Box::new(Mqtt {
            id,
            name: req.name.clone(),
            conf,
            status: Status::Stopped,
            tx: None,
        }))
    }

    async fn run(&self) {
        let mut mqtt_options =
            MqttOptions::new(self.conf.id.clone(), self.conf.host.clone(), self.conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10);
        match client
            .subscribe(self.conf.topic.clone(), QoS::AtMostOnce)
            .await
        {
            Ok(_) => {}
            Err(e) => error!("Failed to connect mqtt server:{}", e),
        }

        let conf = self.conf.clone();
        let tx = match &self.tx {
            Some(tx) => tx.clone(),
            None => panic!("sendoer is none"),
        };

        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
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
                        match client.subscribe(conf.topic.clone(), QoS::AtMostOnce).await {
                            Ok(_) => {}
                            Err(e) => error!("Failed to connect mqtt server:{}", e),
                        }
                    }
                }
            }
        });
    }
}

#[async_trait]
impl Source for Mqtt {
    async fn subscribe(&mut self) -> HaliaResult<Receiver<MessageBatch>> {
        match self.status {
            Status::Running => match &self.tx {
                Some(tx) => Ok(tx.subscribe()),
                None => return Err(HaliaError::IoErr),
            },
            Status::Stopped => {
                let (tx, rx) = broadcast::channel(10);
                let tx1 = tx.clone();
                self.tx = Some(tx);
                self.run().await;
                Ok(rx)
            }
        }
    }

    fn get_info(&self) -> Result<ListSourceResp> {
        Ok(ListSourceResp {
            id: self.id.clone(),
            name: self.name.clone(),
            r#type: "mqtt".to_string(),
        })
    }

    fn get_detail(&self) -> HaliaResult<SourceDetailResp> {
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
}

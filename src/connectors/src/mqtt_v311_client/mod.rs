use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use message::MessageBatch;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::error;
use types::connector::CreateConnectorReq;
use uuid::Uuid;

use crate::Connector;

mod sink;
mod source;

pub(crate) const TYPE: &str = "mqtt_v3.1.1";

pub(crate) struct MqttV311 {
    pub id: Uuid,
    conf: Conf,
    status: bool,

    source_topics: Arc<RwLock<Vec<Topic>>>,
    sink_topics: Vec<Topic>,
    client: Option<AsyncClient>,
}

#[derive(Clone)]
struct Topic {
    pub id: Uuid,
    pub topic: String,
    pub qos: u8,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: u8,
}

#[derive(Serialize, Deserialize)]
struct Conf {
    id: String,
    timeout: usize,
    keep_alive: usize,
    clean_session: bool,
    host: String,
    port: u16,
}

// enum Conf {
//     Password(PasswordConf),
// }

struct PasswordConf {
    username: String,
    password: String,
}

pub fn new(id: Uuid, req: &CreateConnectorReq) -> Result<Connector> {
    let conf: Conf = serde_json::from_value(req.conf.clone())?;
    Ok(Connector::MqttV311(MqttV311 {
        id: Uuid::new_v4(),
        conf,
        source_topics: Arc::new(RwLock::new(vec![])),
        sink_topics: vec![],
        status: false,
        client: None,
    }))
}

impl MqttV311 {
    pub async fn subscribe(&mut self, topic_id: Uuid) -> Result<broadcast::Receiver<MessageBatch>> {
        if !self.status {
            self.run();
        }
        for topic in self.source_topics.write().await.iter_mut() {
            if topic.id == topic_id {
                match &topic.tx {
                    Some(tx) => {
                        let rx = tx.subscribe();
                        return Ok(rx);
                    }
                    None => {
                        // TODO maybe not 10
                        let (tx, rx) = broadcast::channel::<MessageBatch>(10);
                        topic.tx = Some(tx);
                        return Ok(rx);
                    }
                }
            }
        }

        bail!("not find topic id")
    }

    pub fn receive(&self, topic_id: Uuid) -> Result<mpsc::Sender<MessageBatch>> {
        todo!()
    }

    pub fn run(&mut self) {
        let mut mqtt_options =
            MqttOptions::new(self.conf.id.clone(), self.conf.host.clone(), self.conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(5));

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10);
        self.client = Some(client);

        let topics = self.source_topics.clone();
        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        match MessageBatch::from_json(p.payload) {
                            Ok(msg) => {
                                for topic in topics.write().await.iter_mut() {
                                    if matches(&topic.topic, &p.topic) {
                                        match &topic.tx {
                                            Some(tx) => {
                                                let _ = tx.send(msg.clone());
                                            }
                                            None => {}
                                        }
                                    }
                                }
                            }
                            Err(e) => error!("Failed to decode msg:{}", e),
                        }
                    }
                    Ok(_) => (),
                    Err(e) => {
                        match e {
                            rumqttc::ConnectionError::MqttState(_) => todo!(),
                            rumqttc::ConnectionError::NetworkTimeout => todo!(),
                            rumqttc::ConnectionError::FlushTimeout => todo!(),
                            rumqttc::ConnectionError::Tls(_) => todo!(),
                            rumqttc::ConnectionError::Io(_) => todo!(),
                            rumqttc::ConnectionError::ConnectionRefused(_) => todo!(),
                            rumqttc::ConnectionError::NotConnAck(_) => todo!(),
                            rumqttc::ConnectionError::RequestsDone => todo!(),
                        }
                        // if let ConnectionError::Timeout(_) = e {
                        //     continue;
                        // }
                        // match client.subscribe(conf.topic.clone(), QoS::AtMostOnce).await {
                        //     Ok(_) => {}
                        //     Err(e) => error!("Failed to connect mqtt server:{}", e),
                        // }
                    }
                }
            }
        });
    }
}

pub fn matches(topic: &str, filter: &str) -> bool {
    if !topic.is_empty() && topic[..1].contains('$') {
        return false;
    }

    let mut topics = topic.split('/');
    let mut filters = filter.split('/');

    for f in filters.by_ref() {
        // "#" being the last element is validated by the broker with 'valid_filter'
        if f == "#" {
            return true;
        }

        // filter still has remaining elements
        // filter = a/b/c/# should match topci = a/b/c
        // filter = a/b/c/d should not match topic = a/b/c
        let top = topics.next();
        match top {
            Some("#") => return false,
            Some(_) if f == "+" => continue,
            Some(t) if f != t => return false,
            Some(_) => continue,
            None => return false,
        }
    }

    // topic has remaining elements and filter's last element isn't "#"
    if topics.next().is_some() {
        return false;
    }

    true
}

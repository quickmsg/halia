use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use common::error::HaliaResult;
use message::MessageBatch;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tracing::{debug, error};
use types::connector::{
    CreateConnectorReq, SearchConnectorItemResp, SearchSinkResp, SearchSourceResp,
};
use uuid::Uuid;

use crate::{save_sink, Connector};

pub(crate) const TYPE: &str = "mqtt_v3.1.1";

pub(crate) struct MqttV311 {
    name: String,
    pub id: Uuid,
    conf: Conf,
    status: bool,

    sources: Arc<RwLock<Vec<Source>>>,
    sinks: Arc<RwLock<Vec<Sink>>>,
    client: Option<AsyncClient>,
}

#[derive(Deserialize, Serialize)]
struct Source {
    pub id: Uuid,
    pub topic: String,
    pub qos: u8,
    #[serde(skip)]
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: u8,
}

#[derive(Deserialize, Serialize)]
struct Sink {
    #[serde(skip)]
    pub client: Option<AsyncClient>,
    pub id: Uuid,
    pub topic: String,
    pub qos: u8,
    #[serde(skip)]
    pub rx: Option<mpsc::Receiver<MessageBatch>>,
    #[serde(skip)]
    pub tx: Option<mpsc::Sender<MessageBatch>>,
    pub ref_cnt: u8,
}

#[derive(Deserialize)]
struct TopicConf {
    pub topic: String,
    pub qos: u8,
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

pub fn new(id: Uuid, req: CreateConnectorReq) -> Result<Box<dyn Connector>> {
    let conf: Conf = serde_json::from_value(req.conf.clone())?;
    Ok(Box::new(MqttV311 {
        id,
        name: req.name,
        conf,
        sources: Arc::new(RwLock::new(vec![])),
        sinks: Arc::new(RwLock::new(vec![])),
        status: false,
        client: None,
    }))
}

impl MqttV311 {
    pub fn run(&mut self) {
        let mqtt_options =
            MqttOptions::new(self.conf.id.clone(), self.conf.host.clone(), self.conf.port);

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 10);
        self.client = Some(client);

        let topics = self.sources.clone();
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
                            rumqttc::ConnectionError::MqttState(e) => {
                                error!("mqtt connection refused:{:?}", e);
                            }
                            rumqttc::ConnectionError::NetworkTimeout => todo!(),
                            rumqttc::ConnectionError::FlushTimeout => todo!(),
                            rumqttc::ConnectionError::Tls(_) => todo!(),
                            rumqttc::ConnectionError::Io(_) => todo!(),
                            rumqttc::ConnectionError::ConnectionRefused(e) => {
                                error!("mqtt connection refused:{:?}", e);
                            }
                            rumqttc::ConnectionError::NotConnAck(_) => todo!(),
                            rumqttc::ConnectionError::RequestsDone => todo!(),
                        }
                        time::sleep(10 * Duration::SECOND).await;
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

    async fn do_create_source(&self, id: Uuid, topic_conf: TopicConf) -> HaliaResult<()> {
        self.sources.write().await.push(Source {
            id,
            topic: topic_conf.topic,
            qos: topic_conf.qos,
            tx: None,
            ref_cnt: 0,
        });

        Ok(())
    }

    async fn do_create_sink(&self, id: Uuid, topic_conf: TopicConf) -> HaliaResult<()> {
        self.sinks.write().await.push(Sink {
            id,
            topic: topic_conf.topic,
            qos: topic_conf.qos,
            tx: None,
            ref_cnt: 0,
            client: None,
            rx: None,
        });

        Ok(())
    }
}

#[async_trait]
impl Connector for MqttV311 {
    fn get_id(&self) -> &Uuid {
        &self.id
    }

    fn get_info(&self) -> SearchConnectorItemResp {
        SearchConnectorItemResp {
            id: self.id,
            r#type: TYPE,
            name: self.name.clone(),
            conf: serde_json::to_value(&self.conf).unwrap(),
        }
    }

    async fn subscribe(
        &mut self,
        source_id: Option<Uuid>,
    ) -> Result<broadcast::Receiver<MessageBatch>> {
        let source_id = match source_id {
            Some(id) => id,
            None => bail!("mqtt源id为空"),
        };

        if !self.status {
            self.run();
        }

        for source in self.sources.write().await.iter_mut() {
            if source.id == source_id {
                match &source.tx {
                    Some(tx) => {
                        let rx = tx.subscribe();
                        return Ok(rx);
                    }
                    None => {
                        // TODO maybe not 10
                        let (tx, rx) = broadcast::channel::<MessageBatch>(10);
                        source.tx = Some(tx);
                        return Ok(rx);
                    }
                }
            }
        }

        bail!("not find topic id")
    }

    async fn create_source(&self, req: &Bytes) -> HaliaResult<()> {
        let topic_conf: TopicConf = serde_json::from_slice(req)?;
        let source_id = Uuid::new_v4();
        super::save_source(&self.id, &source_id, req).await?;
        self.do_create_source(source_id, topic_conf).await
    }

    async fn recover_source(&self, id: Uuid, req: String) {
        let topic_conf: TopicConf = serde_json::from_str(&req).unwrap();
        let _ = self.do_create_source(id, topic_conf).await;
    }

    async fn search_sources(&self, page: usize, size: usize) -> HaliaResult<SearchSourceResp> {
        let mut total = 0;
        let mut i = 0;
        let mut data = vec![];
        for topic in self.sources.read().await.iter() {
            if i >= (page - 1) * size && i < page * size {
                data.push(serde_json::to_value(topic).unwrap());
            }
            total += 1;
            i += 1;
        }

        Ok(SearchSourceResp { total, data })
    }

    async fn create_sink(&self, req: &Bytes) -> HaliaResult<()> {
        let topic_conf: TopicConf = serde_json::from_slice(req)?;
        let sink_id = Uuid::new_v4();
        save_sink(&self.id, &sink_id, req).await?;
        self.do_create_sink(sink_id, topic_conf).await
    }

    async fn recover_sink(&self, id: Uuid, req: String) {
        let topic_conf: TopicConf = serde_json::from_str(&req).unwrap();
        let _ = self.do_create_sink(id, topic_conf).await;
    }

    async fn search_sinks(&self, page: usize, size: usize) -> HaliaResult<SearchSinkResp> {
        let mut total = 0;
        let mut i = 0;
        let mut data = vec![];
        for sink in self.sinks.read().await.iter() {
            if i >= (page - 1) * size && i < page * size {
                data.push(serde_json::to_value(sink).unwrap());
            }
            total += 1;
            i += 1;
        }

        Ok(SearchSinkResp { total, data })
    }

    async fn publish(&mut self, sink_id: Option<Uuid>) -> Result<mpsc::Sender<MessageBatch>> {
        let sink_id = match sink_id {
            Some(id) => id,
            None => bail!("mqtt动作id为空"),
        };

        if !self.status {
            self.run();
        }

        for sink in self.sinks.write().await.iter_mut() {
            if sink.id == sink_id {
                match &sink.tx {
                    Some(tx) => {
                        return Ok(tx.clone());
                    }
                    None => {
                        let (tx, rx) = mpsc::channel::<MessageBatch>(10);
                        if sink.client.is_none() {
                            sink.client = Some(self.client.as_ref().unwrap().clone());
                        }
                        sink.run(rx);
                        let tx_clone = tx.clone();
                        sink.tx = Some(tx);
                        return Ok(tx_clone);
                    }
                }
            }
        }

        bail!("not find topic id")
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

impl Sink {
    fn run(&self, mut rx: mpsc::Receiver<MessageBatch>) {
        let client = self.client.as_ref().unwrap().clone();
        let topic = self.topic.clone();
        let qos = self.qos;
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
}

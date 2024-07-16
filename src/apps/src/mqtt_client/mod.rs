use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use sink::Sink;
use source::Source;
use tokio::{
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tracing::error;
use types::apps::{
    mqtt_client::{
        CreateUpdateMqttClientReq, CreateUpdateSinkReq, CreateUpdateSourceReq, SearchSinksResp,
        SearchSourcesResp,
    },
    SearchAppItemResp,
};
use uuid::Uuid;

pub const TYPE: &str = "mqtt_client";

pub mod manager;
mod sink;
mod source;

pub struct MqttClient {
    pub id: Uuid,
    conf: CreateUpdateMqttClientReq,
    on: bool,

    sources: Arc<RwLock<Vec<Source>>>,
    sinks: Arc<RwLock<Vec<Sink>>>,
    client: Option<AsyncClient>,
}

impl MqttClient {
    pub async fn new(app_id: Option<Uuid>, req: CreateUpdateMqttClientReq) -> HaliaResult<Self> {
        let (app_id, new) = match app_id {
            Some(app_id) => (app_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {}

        Ok(Self {
            id: app_id,
            conf: req,
            sources: Arc::new(RwLock::new(vec![])),
            sinks: Arc::new(RwLock::new(vec![])),
            on: false,
            client: None,
        })
    }

    pub async fn update(&mut self, req: CreateUpdateMqttClientReq) -> HaliaResult<()> {
        todo!()
    }

    pub async fn start(&mut self) {
        self.on = true;
        let mqtt_options =
            MqttOptions::new(self.conf.id.clone(), self.conf.host.clone(), self.conf.port);

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 16);
        let sources = self.sources.clone();
        for source in sources.read().await.iter() {
            let _ = client
                .subscribe(source.conf.topic.clone(), get_mqtt_qos(source.conf.qos))
                .await;
        }
        self.client = Some(client);

        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        match MessageBatch::from_json(p.payload) {
                            Ok(msg) => {
                                for source in sources.write().await.iter_mut() {
                                    if matches(&source.conf.topic, &p.topic) {
                                        match &source.tx {
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
                            rumqttc::ConnectionError::Io(e) => {
                                error!("mqtt connection refused:{:?}", e);
                            }
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

    pub async fn delete(&mut self) -> HaliaResult<()> {
        todo!()
    }
}

impl MqttClient {
    fn search(&self) -> SearchAppItemResp {
        SearchAppItemResp {
            id: self.id,
            r#type: TYPE,
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

        if !self.on {
            self.start().await;
        }

        for source in self.sources.write().await.iter_mut() {
            if source.id == source_id {
                match &source.tx {
                    Some(tx) => {
                        let rx = tx.subscribe();
                        return Ok(rx);
                    }
                    None => {
                        let (tx, rx) = broadcast::channel::<MessageBatch>(16);
                        source.tx = Some(tx);
                        return Ok(rx);
                    }
                }
            }
        }

        bail!("not find topic id")
    }

    pub async fn create_source(
        &self,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceReq,
    ) -> HaliaResult<()> {
        match Source::new(&self.id, source_id, req).await {
            Ok(source) => {
                if self.on {
                    if let Err(e) = self
                        .client
                        .as_ref()
                        .unwrap()
                        .subscribe(source.conf.topic.clone(), get_mqtt_qos(source.conf.qos))
                        .await
                    {
                        error!("client subscribe err:{e}");
                    }
                }

                self.sources.write().await.push(source);

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sources(&self, page: usize, size: usize) -> HaliaResult<SearchSourcesResp> {
        let mut i = 0;
        let mut data = vec![];
        for source in self
            .sources
            .read()
            .await
            .iter()
            .rev()
            .skip((page - 1) * size)
        {
            data.push(source.search());
            i += 1;
            if i == size {
                break;
            }
        }

        Ok(SearchSourcesResp {
            total: self.sources.read().await.len(),
            data,
        })
    }

    async fn update_source(&self, source_id: Uuid, req: CreateUpdateSourceReq) -> HaliaResult<()> {
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => match source.update(&self.id, req).await {
                Ok(restart) => {
                    if self.on && restart {
                        if let Err(e) = self
                            .client
                            .as_ref()
                            .unwrap()
                            .unsubscribe(source.conf.topic.clone())
                            .await
                        {
                            error!("unsubscribe err:{e}");
                        }

                        if let Err(e) = self
                            .client
                            .as_ref()
                            .unwrap()
                            .subscribe(source.conf.topic.clone(), get_mqtt_qos(source.conf.qos))
                            .await
                        {
                            error!("subscribe err:{e}");
                        }
                    }

                    Ok(())
                }
                Err(e) => Err(e),
            },
            None => Err(HaliaError::ProtocolNotSupported),
        }
    }

    async fn create_sink(
        &self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(&self.id, sink_id, req).await {
            Ok(sink) => {
                todo!()
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sinks(&self, page: usize, size: usize) -> HaliaResult<SearchSinksResp> {
        // let mut total = 0;
        // let mut i = 0;
        // let mut data = vec![];
        // for sink in self.sinks.read().await.iter() {
        //     if i >= (page - 1) * size && i < page * size {
        //         data.push(serde_json::to_value(sink).unwrap());
        //     }
        //     total += 1;
        //     i += 1;
        // }

        // Ok(SearchSinksResp { total, data })
        todo!()
    }

    async fn publish(&mut self, sink_id: &Option<Uuid>) -> Result<mpsc::Sender<MessageBatch>> {
        let sink_id = match sink_id {
            Some(id) => id,
            None => bail!("mqtt动作id为空"),
        };

        if !self.on {
            self.start().await;
        }

        for sink in self.sinks.write().await.iter_mut() {
            if sink.id == *sink_id {
                match &sink.tx {
                    Some(tx) => {
                        // return Ok(tx.clone());
                        todo!()
                    }
                    None => {
                        let (tx, rx) = mpsc::channel::<MessageBatch>(10);
                        if sink.client.is_none() {
                            sink.client = Some(self.client.as_ref().unwrap().clone());
                        }
                        sink.start(rx);
                        let tx_clone = tx.clone();
                        // sink.tx = Some(tx);
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

impl Sink {}

fn get_mqtt_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtLeastOnce,
        1 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => unreachable!(),
    }
}

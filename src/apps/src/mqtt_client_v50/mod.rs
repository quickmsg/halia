use std::{str::FromStr, sync::Arc, time::Duration};

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use rumqttc::v5::{AsyncClient, Event, Incoming, MqttOptions};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tracing::error;
use types::apps::{
    mqtt_client_v311::{
        CreateUpdateMqttClientReq, CreateUpdateSinkReq, CreateUpdateSourceReq, SearchSinksResp,
        SearchSourcesResp,
    },
    SearchAppsItemResp,
};
use uuid::Uuid;

pub const TYPE: &str = "mqtt_client_v50";

pub mod manager;
mod sink;
mod source;

pub struct MqttClient {
    pub id: Uuid,
    conf: CreateUpdateMqttClientReq,
    stop_signal_tx: Option<mpsc::Sender<()>>,

    sources: Arc<RwLock<Vec<Source>>>,
    sinks: Vec<Sink>,
    client: Option<Arc<AsyncClient>>,
    ref_cnt: usize,
}

impl MqttClient {
    pub async fn new(app_id: Option<Uuid>, req: CreateUpdateMqttClientReq) -> HaliaResult<Self> {
        let (app_id, new) = match app_id {
            Some(app_id) => (app_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::apps::mqtt_client_v311::create(
                &app_id,
                TYPE,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: app_id,
            conf: req,
            sources: Arc::new(RwLock::new(vec![])),
            sinks: vec![],
            client: None,
            stop_signal_tx: None,
            ref_cnt: 0,
        })
    }

    pub async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::apps::mqtt_client_v311::read_sources(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);
                    let source_id = Uuid::from_str(items[0]).unwrap();
                    self.create_source(Some(source_id), serde_json::from_str(items[1]).unwrap())
                        .await?;
                }
            }
            Err(e) => return Err(e.into()),
        }

        match persistence::apps::mqtt_client_v311::read_sinks(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);
                    let sink_id = Uuid::from_str(items[0]).unwrap();
                    self.create_sink(Some(sink_id), serde_json::from_str(items[1]).unwrap())
                        .await?;
                }
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    pub async fn update(&mut self, req: CreateUpdateMqttClientReq) -> HaliaResult<()> {
        persistence::apps::update_app(&self.id, serde_json::to_string(&req).unwrap()).await?;

        let mut restart = false;
        if self.conf.client_id != req.client_id
            || self.conf.timeout != req.timeout
            || self.conf.keep_alive != req.keep_alive
            || self.conf.clean_session != req.clean_session
            || self.conf.host != req.host
            || self.conf.port != req.port
        {
            restart = true;
        }
        self.conf = req;

        if self.stop_signal_tx.is_some() && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            self.start().await;
            for sink in self.sinks.iter_mut() {
                sink.restart(self.client.as_ref().unwrap().clone()).await;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) {
        let mqtt_options = MqttOptions::new(
            self.conf.client_id.clone(),
            self.conf.host.clone(),
            self.conf.port,
        );

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 16);
        let sources = self.sources.clone();
        for source in sources.read().await.iter() {
            let _ = client
                .subscribe(source.conf.topic.clone(), get_mqtt_qos(source.conf.qos))
                .await;
        }
        self.client = Some(Arc::new(client));

        let (tx, mut rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(tx);

        tokio::spawn(async move {
            loop {
                select! {
                    _ = rx.recv() => {
                        return
                    }

                    event = event_loop.poll() => {
                        match event {
                            Ok(Event::Incoming(Incoming::Publish(p))) => {
                                match MessageBatch::from_json(p.payload) {
                                    Ok(msg) => {
                                        for source in sources.write().await.iter_mut() {
                                            if matches(&source.conf.topic, p.topic.as_str()) {
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
                                    rumqttc::v5::ConnectionError::MqttState(_) => todo!(),
                                    rumqttc::v5::ConnectionError::Timeout(_) => todo!(),
                                    rumqttc::v5::ConnectionError::Tls(_) => todo!(),
                                    rumqttc::v5::ConnectionError::Io(_) => todo!(),
                                    rumqttc::v5::ConnectionError::ConnectionRefused(_) => todo!(),
                                    rumqttc::v5::ConnectionError::NotConnAck(_) => todo!(),
                                    rumqttc::v5::ConnectionError::RequestsDone => todo!(),
                                }
                                time::sleep(10 * Duration::SECOND).await;
                            }
                        }
                    }
                }
            }
        });
    }

    async fn stop(&mut self) {
        // TODO 验证引用
        for sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;
        self.client = None;
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        todo!()
    }

    fn search(&self) -> SearchAppsItemResp {
        SearchAppsItemResp {
            id: self.id,
            r#type: TYPE,
            conf: serde_json::to_value(&self.conf).unwrap(),
        }
    }

    async fn subscribe(
        &mut self,
        source_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        let rx = match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => source.subscribe(),
            None => return Err(HaliaError::NotFound),
        };

        self.start().await;
        self.ref_cnt += 1;
        Ok(rx)
    }

    async fn unsubscribe(&mut self, source_id: &Uuid) -> HaliaResult<()> {
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => {
                source.unsubscribe();
                self.ref_cnt -= 1;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_source(
        &self,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceReq,
    ) -> HaliaResult<()> {
        match Source::new(&self.id, source_id, req).await {
            Ok(source) => {
                if self.stop_signal_tx.is_some() {
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
            if data.len() == size {
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
                    if self.stop_signal_tx.is_some() && restart {
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

    async fn delete_source(&self, source_id: Uuid) -> HaliaResult<()> {
        match self
            .sources
            .read()
            .await
            .iter()
            .find(|source| source.id == source_id)
        {
            Some(source) => source.delete(&self.id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(&self.id, sink_id, req).await {
            Ok(sink) => {
                self.sinks.push(sink);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp {
        let mut data = vec![];
        for sink in self.sinks.iter().rev().skip((page - 1) * size) {
            data.push(sink.search());
            if data.len() == size {
                break;
            }
        }
        SearchSinksResp {
            total: self.sinks.len(),
            data,
        }
    }

    pub async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => match sink.update(&self.id, req).await {
                Ok(restart) => {
                    if sink.stop_signal_tx.is_some() && restart {
                        sink.restart(self.client.as_ref().unwrap().clone()).await;
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            },
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id == sink_id);
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn publish(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        if self.stop_signal_tx.is_none() {
            self.start().await;
        }

        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => {
                if sink.stop_signal_tx.is_none() {
                    sink.start(self.client.as_ref().unwrap().clone());
                }

                Ok(sink.tx.as_ref().unwrap().clone())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    async fn unpublish(&mut self, sink_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.unpublish().await),
            None => Err(HaliaError::NotFound),
        }
    }

    async fn add_ref_cnt(&mut self) {
        self.ref_cnt += 1;
        if self.stop_signal_tx.is_none() {
            self.start().await;
        }
    }

    async fn sub_ref_cnt(&mut self) {
        self.ref_cnt -= 1;
        if self.ref_cnt == 0 {
            self.stop().await;
        }
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

fn get_mqtt_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtLeastOnce,
        1 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => unreachable!(),
    }
}

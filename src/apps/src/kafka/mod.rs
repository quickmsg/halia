use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use rskafka::client::{
    consumer::StartOffset,
    partition::{Compression, UnknownTopicHandling},
    Client, ClientBuilder,
};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{broadcast, mpsc, watch, RwLock},
    time,
};
use types::apps::kafka::{KafkaConf, SinkConf, SourceConf};

use crate::App;

mod sink;
mod source;

pub struct Kafka {
    id: String,
    err: Option<String>,
    stop_signal_tx: watch::Sender<()>,

    kafka_client: Arc<RwLock<Option<Client>>>,

    sources: DashMap<String, Source>,
    stopped_sources: Option<Vec<(String, SourceConf)>>,
    sinks: DashMap<String, Sink>,
    stopped_sinks: Option<Vec<(String, SinkConf)>>,
}

pub fn validate_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}

pub fn validate_source_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SourceConf = serde_json::from_value(conf.clone())?;
    Source::validate_conf(&conf)?;
    Ok(())
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: KafkaConf = serde_json::from_value(conf).unwrap();
    let kafka_client = Arc::new(RwLock::new(None));
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());

    let (connected_signal_tx, connected_signal_rx) = watch::channel(());
    Kafka::connect_loop(
        id.clone(),
        kafka_client.clone(),
        &conf,
        stop_signal_rx,
        connected_signal_tx,
    );
    Box::new(Kafka {
        id,
        err: None,
        sources: DashMap::new(),
        stopped_sources: None,
        sinks: DashMap::new(),
        stopped_sinks: None,
        kafka_client,
        stop_signal_tx,
    })
}

impl Kafka {
    fn connect_loop(
        id: String,
        kafka_client: Arc<RwLock<Option<Client>>>,
        conf: &KafkaConf,
        mut stop_signal_rx: watch::Receiver<()>,
        connect_signal_tx: watch::Sender<()>,
    ) {
        let bootstrap_brokers = conf.bootstrap_brokers.clone();
        let reconnect = conf.reconnect;
        tokio::spawn(async move {
            loop {
                match ClientBuilder::new(bootstrap_brokers.clone()).build().await {
                    Ok(client) => {
                        kafka_client.write().await.replace(client);
                        connect_signal_tx.send(()).unwrap();
                        events::insert_connect(types::events::ResourceType::App, &id).await;
                    }
                    Err(e) => {
                        events::insert_disconnect(
                            types::events::ResourceType::App,
                            &id,
                            e.to_string(),
                        )
                        .await;
                        let sleep = time::sleep(Duration::from_secs(reconnect));
                        tokio::pin!(sleep);
                        select! {
                            _ = stop_signal_rx.changed() => {
                                return stop_signal_rx;
                            }

                            _ = &mut sleep => {}
                        }
                    }
                }
            }
        });
    }

    fn event_loop(
        mut stop_signal_rx: watch::Receiver<()>,
        mut connected_signal_rx: watch::Receiver<()>,
    ) {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return;
                    }

                    _ = connected_signal_rx.changed() => {
                        return;
                    }
                }
            }
        });
    }
}

#[async_trait]
impl App for Kafka {
    async fn update(
        &mut self,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
    }

    async fn stop(&mut self) {
        self.stop_signal_tx.send(()).unwrap();

        for mut source in self.sources.iter_mut() {
            source.stop().await;
        }

        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        // todo disconenct kafka client
    }

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf.clone())?;
        match self.kafka_client.read().await.as_ref() {
            Some(client) => {
                let source = Source::new(client, conf).await?;
                self.sources.insert(source_id, source);
            }
            None => match self.stopped_sources.as_mut() {
                Some(stopped_sources) => {
                    stopped_sources.push((source_id, conf));
                }
                None => {
                    self.stopped_sources = Some(vec![(source_id, conf)]);
                }
            },
        }

        Ok(())
    }

    async fn update_source(
        &mut self,
        source_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        match self.kafka_client.read().await.as_ref() {
            Some(_) => match self.sources.get_mut(&source_id) {
                Some(mut source) => {
                    let old_conf: SourceConf = serde_json::from_value(old_conf)?;
                    let new_conf: SourceConf = serde_json::from_value(new_conf)?;
                    source.update_conf(old_conf, new_conf).await?;
                    Ok(())
                }
                None => return Err(HaliaError::NotFound(source_id)),
            },
            None => {
                todo!()
                //  todo
            }
        }
    }

    async fn delete_source(&mut self, source_id: String) -> HaliaResult<()> {
        match self.sources.remove(&source_id) {
            Some((_, mut source)) => {
                source.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id)),
        }
    }

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.sources.get(source_id) {
            Some(source) => Ok(source.mb_tx.subscribe()),
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf.clone())?;
        // let sink = Sink::new("servers".to_owned(), conf).await?;
        // self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        match self.sinks.get_mut(&sink_id) {
            Some(mut sink) => {
                let old_conf: SinkConf = serde_json::from_value(old_conf)?;
                let new_conf: SinkConf = serde_json::from_value(new_conf)?;
                sink.update_conf(old_conf, new_conf).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id)),
        }
    }

    async fn delete_sink(&mut self, sink_id: String) -> HaliaResult<()> {
        match self.sinks.remove(&sink_id) {
            Some((_, mut sink)) => {
                sink.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id)),
        }
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

fn tansfer_unknown_topic_handling(
    unknown_topic_handling: types::apps::kafka::UnknownTopicHandling,
) -> UnknownTopicHandling {
    match unknown_topic_handling {
        types::apps::kafka::UnknownTopicHandling::Error => {
            rskafka::client::partition::UnknownTopicHandling::Error
        }
        types::apps::kafka::UnknownTopicHandling::Retry => {
            rskafka::client::partition::UnknownTopicHandling::Retry
        }
    }
}

fn transfer_compression(compression: types::apps::kafka::Compression) -> Compression {
    match compression {
        types::apps::kafka::Compression::None => Compression::NoCompression,
        types::apps::kafka::Compression::Gzip => Compression::Gzip,
        types::apps::kafka::Compression::Lz4 => Compression::Lz4,
        types::apps::kafka::Compression::Snappy => Compression::Snappy,
        types::apps::kafka::Compression::Zstd => Compression::Zstd,
    }
}

fn transfer_start_offset(start_offset: types::apps::kafka::StartOffset) -> StartOffset {
    match start_offset {
        types::apps::kafka::StartOffset::Earliest => StartOffset::Earliest,
        types::apps::kafka::StartOffset::Latest => StartOffset::Latest,
        types::apps::kafka::StartOffset::At(offset) => StartOffset::At(offset),
    }
}

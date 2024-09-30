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
use tracing::debug;
use types::apps::kafka::{KafkaConf, SinkConf, SourceConf};

use crate::App;

mod sink;
mod source;

pub struct Kafka {
    _err: Option<String>,
    stop_signal_tx: watch::Sender<()>,

    kafka_client: Arc<RwLock<Option<Client>>>,

    sources: Arc<DashMap<String, Source>>,
    sinks: Arc<DashMap<String, Sink>>,
    kafka_err_tx: mpsc::Sender<String>,
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

    let sources = Arc::new(DashMap::new());
    let sinks = Arc::new(DashMap::new());
    let (kafka_err_tx, kafka_err_rx) = mpsc::channel(1);
    Kafka::event_loop(
        id.clone(),
        conf,
        kafka_client.clone(),
        stop_signal_rx,
        kafka_err_rx,
        sources.clone(),
        sinks.clone(),
    );

    Box::new(Kafka {
        _err: None,
        sources,
        sinks,
        kafka_client,
        stop_signal_tx,
        kafka_err_tx,
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
        id: String,
        conf: KafkaConf,
        kafka_client: Arc<RwLock<Option<Client>>>,
        mut stop_signal_rx: watch::Receiver<()>,
        mut kafka_err_rx: mpsc::Receiver<String>,
        sources: Arc<DashMap<String, Source>>,
        sinks: Arc<DashMap<String, Sink>>,
    ) {
        let (connect_signal_tx, mut connect_signal_rx) = watch::channel(());
        Self::connect_loop(
            id,
            kafka_client.clone(),
            &conf,
            stop_signal_rx.clone(),
            connect_signal_tx,
        );

        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return;
                    }

                    e = kafka_err_rx.recv() => {
                        debug!("Kafka error received, {:?}", e);
                    }

                    _ = connect_signal_rx.changed() => {
                        Self::handle_connect_status_changed(&kafka_client, &sources, &sinks).await;
                    }
                }
            }
        });
    }

    async fn handle_connect_status_changed(
        kafka_client: &Arc<RwLock<Option<Client>>>,
        sources: &Arc<DashMap<String, Source>>,
        sinks: &Arc<DashMap<String, Sink>>,
    ) {
        let kafka_client = kafka_client.read().await;
        for mut source in sources.iter_mut() {
            source.update_kafka_client(kafka_client.as_ref()).await;
        }
        for mut sink in sinks.iter_mut() {
            sink.update_kafka_client(kafka_client.as_ref()).await;
        }
    }
}

#[async_trait]
impl App for Kafka {
    async fn update(
        &mut self,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        self.stop_signal_tx.send(()).unwrap();
        // Self::connect_loop(id, kafka_client, conf, stop_signal_rx, connect_signal_tx);
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
        let source = Source::new(
            self.kafka_client.read().await.as_ref(),
            self.kafka_err_tx.clone(),
            conf,
        )
        .await;
        self.sources.insert(source_id, source);

        Ok(())
    }

    async fn update_source(
        &mut self,
        source_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        match self.sources.get_mut(&source_id) {
            Some(mut source) => {
                let old_conf: SourceConf = serde_json::from_value(old_conf)?;
                let new_conf: SourceConf = serde_json::from_value(new_conf)?;
                source
                    .update_conf(self.kafka_client.read().await.as_ref(), old_conf, new_conf)
                    .await;
                Ok(())
            }
            None => return Err(HaliaError::NotFound(source_id)),
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
        let sink = Sink::new(self.kafka_client.read().await.as_ref(), conf).await;
        self.sinks.insert(sink_id, sink);

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
                sink.update_conf(self.kafka_client.read().await.as_ref(), old_conf, new_conf)
                    .await;
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

fn transfer_unknown_topic_handling(
    unknown_topic_handling: &types::apps::kafka::UnknownTopicHandling,
) -> UnknownTopicHandling {
    match unknown_topic_handling {
        types::apps::kafka::UnknownTopicHandling::Error => UnknownTopicHandling::Error,
        types::apps::kafka::UnknownTopicHandling::Retry => UnknownTopicHandling::Retry,
    }
}

fn transfer_compression(compression: &types::apps::kafka::Compression) -> Compression {
    match compression {
        types::apps::kafka::Compression::None => Compression::NoCompression,
        types::apps::kafka::Compression::Gzip => Compression::Gzip,
        types::apps::kafka::Compression::Lz4 => Compression::Lz4,
        types::apps::kafka::Compression::Snappy => Compression::Snappy,
        types::apps::kafka::Compression::Zstd => Compression::Zstd,
    }
}

fn transfer_start_offset(start_offset: &types::apps::kafka::StartOffset) -> StartOffset {
    match start_offset {
        types::apps::kafka::StartOffset::Earliest => StartOffset::Earliest,
        types::apps::kafka::StartOffset::Latest => StartOffset::Latest,
        types::apps::kafka::StartOffset::At(offset) => StartOffset::At(*offset),
    }
}

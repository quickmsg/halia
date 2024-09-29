use std::sync::Arc;

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use rdkafka::client::Client;
use sink::Sink;
use tokio::sync::{mpsc, watch, RwLock};
use types::apps::kafka::{KafkaConf, SinkConf};

use crate::App;

mod sink;

pub struct Kafka {
    id: String,
    err: Option<String>,
    sinks: DashMap<String, Sink>,
    kafka_client: Arc<RwLock<Option<Arc<Client>>>>,

    stop_signal_tx: watch::Sender<()>,
}

pub fn validate_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
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
    // Kafka::connect(id.clone(), kafka_client.clone(), conf, stop_signal_rx);
    Box::new(Kafka {
        id,
        err: None,
        sinks: DashMap::new(),
        kafka_client,
        stop_signal_tx,
    })
}

impl Kafka {
    // fn connect(
    //     app_id: String,
    //     kafka_client: Arc<RwLock<Option<Arc<Client>>>>,
    //     conf: KafkaConf,
    //     mut stop_signal_rx: watch::Receiver<()>,
    // ) {
    //     tokio::spawn(async move {
    //         loop {
    //             let brokers = conf.bootstrap_brokers.join(",");
    //             match ClientConfig::new()
    //                 .set("bootstrap.servers", brokers)
    //                 .set("message.timeout.ms", "5000")
    //                 .create::<FutureProducer>()
    //             {
    //                 Ok(client) => {
    //                     events::insert_connect(types::events::ResourceType::App, &app_id).await;
    //                     kafka_client.write().await.replace(Arc::new(client));
    //                 }
    //                 Err(e) => {
    //                     events::insert_disconnect(
    //                         types::events::ResourceType::App,
    //                         &app_id,
    //                         e.to_string(),
    //                     )
    //                     .await;

    //                     let sleep = time::sleep(Duration::from_secs(conf.reconnect));
    //                     tokio::pin!(sleep);
    //                     select! {
    //                         _ = stop_signal_rx.changed() => {
    //                             return stop_signal_rx;
    //                         }

    //                         _ = &mut sleep => {}
    //                     }
    //                 }
    //             }
    //         }
    //     });

    //     todo!()
    // }
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
        todo!()
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf.clone())?;
        let sink = Sink::new("servers".to_owned(), conf).await?;
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

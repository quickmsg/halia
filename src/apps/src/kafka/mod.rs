use async_trait::async_trait;
use common::error::HaliaResult;
use dashmap::DashMap;
use message::MessageBatch;
use rskafka::client::{partition::UnknownTopicHandling, ClientBuilder};
use sink::Sink;
use tokio::sync::mpsc;
use types::apps::kafka::SinkConf;

use crate::App;

mod sink;

pub struct KafkaClient {
    id: String,
    err: Option<String>,
    sinks: DashMap<String, Sink>,
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
    Box::new(KafkaClient {
        id,
        err: None,
        sinks: DashMap::new(),
    })
}

impl KafkaClient {
    async fn event_loop() {
        let connection = "localhost:9093".to_owned();
        let client = ClientBuilder::new(vec![connection]).build().await.unwrap();
       
        todo!()
    }
}

#[async_trait]
impl App for KafkaClient {
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
        todo!()
    }

    async fn update_sink(
        &mut self,
        sink_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_sink(&mut self, sink_id: String) -> HaliaResult<()> {
        todo!()
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        todo!()
    }
}

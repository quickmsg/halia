use std::sync::Arc;

use async_trait::async_trait;
use common::error::HaliaResult;
use dashmap::DashMap;
use message::MessageBatch;
use sink::Sink;
use taos::{AsyncQueryable, AsyncTBuilder, Taos, TaosBuilder};
use tokio::sync::mpsc;
use tracing::warn;
use types::apps::tdengine::{SinkConf, TDengineConf};

use crate::App;

mod sink;

pub struct TDengine {
    conf: Arc<TDengineConf>,
    sinks: DashMap<String, Sink>,
}

pub fn validate_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}

pub fn validate_sink_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: TDengineConf = serde_json::from_value(conf).unwrap();

    Box::new(TDengine {
        conf: Arc::new(conf),
        sinks: DashMap::new(),
    })
}

#[async_trait]
impl App for TDengine {
    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        _new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        Ok(())
    }

    async fn stop(&mut self) {
        todo!()
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf.clone())?;
        let sink = Sink::new(conf, self.conf.clone()).await;
        self.sinks.insert(sink_id, sink);
        Ok(())
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

async fn new_tdengine_client(tdengine_conf: &Arc<TDengineConf>, sink_conf: &SinkConf) -> Taos {
    let taos = TaosBuilder::from_dsn(format!(
        "taos://{}:{}",
        tdengine_conf.host, tdengine_conf.port
    ))
    .unwrap()
    .build()
    .await
    .unwrap();
    if let Err(e) = taos.exec(format!("USE `{}`", sink_conf.db)).await {
        warn!("{}", e);
    }

    taos
}

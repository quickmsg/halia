use std::sync::Arc;

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use sink::Sink;
use source::Source;
use tokio::sync::{broadcast, mpsc};
use types::apps::{
    http_client::{HttpClientConf, SinkConf, SourceConf},
    AppConf,
};

use crate::App;

mod sink;
mod source;

pub struct HttpClient {
    id: String,
    conf: Arc<HttpClientConf>,
    err: Option<String>,
    sources: DashMap<String, Source>,
    sinks: DashMap<String, Sink>,
}

pub fn new(app_id: String, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: HttpClientConf = serde_json::from_value(app_conf.ext)?;

    Ok(Box::new(HttpClient {
        id: app_id,
        conf: Arc::new(ext_conf),
        err: None,
        sources: DashMap::new(),
        sinks: DashMap::new(),
    }))
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

#[async_trait]
impl App for HttpClient {
    async fn update(
        &mut self,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let old_conf: HttpClientConf = serde_json::from_value(old_conf)?;
        let new_conf: HttpClientConf = serde_json::from_value(new_conf)?;

        if old_conf == new_conf {
            return Ok(());
        }

        self.conf = Arc::new(new_conf);

        for mut source in self.sources.iter_mut() {
            source.update_http_client(self.conf.clone()).await;
        }

        for mut sink in self.sinks.iter_mut() {
            sink.update_http_client(self.conf.clone()).await;
        }

        Ok(())
    }

    async fn stop(&mut self) {
        for mut source in self.sources.iter_mut() {
            source.stop().await;
        }
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
    }

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(self.conf.clone(), conf).await;
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
                source.update_conf(old_conf, new_conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id)),
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

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(self.conf.clone(), conf);
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
                sink.update_conf(old_conf, new_conf).await;
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

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.sources.get(source_id) {
            Some(source) => Ok(source.mb_tx.subscribe()),
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

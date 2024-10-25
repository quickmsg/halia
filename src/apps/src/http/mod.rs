use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use reqwest::{Certificate, Client, ClientBuilder, Identity, RequestBuilder};
use sink::Sink;
use source::Source;
use tokio::sync::mpsc;
use types::apps::{
    http_client::{BasicAuth, HttpClientConf, SinkConf, SourceConf},
    SearchAppsItemRunningInfo,
};

use crate::App;

mod sink;
mod source;

pub struct HttpClient {
    _id: String,
    conf: Arc<HttpClientConf>,
    err: Option<String>,
    sources: DashMap<String, Source>,
    sinks: DashMap<String, Sink>,
    rtt: AtomicU16,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: HttpClientConf = serde_json::from_value(conf).unwrap();

    Box::new(HttpClient {
        _id: id,
        conf: Arc::new(conf),
        err: None,
        sources: DashMap::new(),
        sinks: DashMap::new(),
        rtt: AtomicU16::new(0),
    })
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
    async fn read_running_info(&self) -> SearchAppsItemRunningInfo {
        SearchAppsItemRunningInfo {
            err: self.err.clone(),
            rtt: self.rtt.load(Ordering::SeqCst),
        }
    }

    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: HttpClientConf = serde_json::from_value(new_conf)?;

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
    ) -> HaliaResult<mpsc::UnboundedReceiver<RuleMessageBatch>> {
        match self.sources.get_mut(source_id) {
            Some(mut source) => Ok(source.get_rx().await),
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn get_sink_tx(
        &self,
        sink_id: &String,
    ) -> HaliaResult<mpsc::UnboundedSender<RuleMessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

fn build_client(ca: Vec<u8>) -> Client {
    let mut builder = ClientBuilder::new();
    builder = builder.add_root_certificate(Certificate::from_pem(&ca).unwrap());
    builder = builder.identity(Identity::from_pem(todo!()).unwrap());
    let client = builder.build().unwrap();
    client
}

fn build_basic_auth(
    mut builder: RequestBuilder,
    basic_auth_item: &Option<BasicAuth>,
    basic_auth_client: &Option<BasicAuth>,
) -> RequestBuilder {
    match (basic_auth_item, basic_auth_client) {
        (None, None) => {}
        (None, Some(basic_auth_client)) => {
            builder = builder.basic_auth(
                basic_auth_client.username.clone(),
                basic_auth_client.password.clone(),
            );
        }
        (Some(basic_auth_item), None) => {
            builder = builder.basic_auth(
                basic_auth_item.username.clone(),
                basic_auth_item.password.clone(),
            );
        }
        (Some(basic_auth_item), Some(_)) => {
            builder = builder.basic_auth(
                basic_auth_item.username.clone(),
                basic_auth_item.password.clone(),
            );
        }
    }

    builder
}

fn build_headers(
    mut builder: RequestBuilder,
    headers_item: &Option<Vec<(String, String)>>,
    headers_client: &Option<Vec<(String, String)>>,
) -> RequestBuilder {
    match (headers_item, headers_client) {
        (None, None) => {}
        (None, Some(headers_client)) => {
            for (k, v) in headers_client {
                builder = builder.header(k, v);
            }
        }
        (Some(headers_item), None) => {
            for (k, v) in headers_item {
                builder = builder.header(k, v);
            }
        }
        (Some(headers_item), Some(headers_client)) => {
            for (k, v) in headers_client {
                if headers_item.iter().find(|(key, _)| key == k).is_none() {
                    builder = builder.header(k, v);
                }
            }
        }
    }

    builder
}

use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use reqwest::header::HOST;
use sink::Sink;
use source::Source;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest as _, handshake::client::Request};
use types::apps::{
    websocket::{AppConf, SinkConf, SourceConf},
    SearchAppsItemRunningInfo,
};

use crate::App;

mod sink;
mod source;

pub struct Websocket {
    _id: String,
    conf: Arc<AppConf>,
    sources: DashMap<String, Source>,
    sinks: DashMap<String, Sink>,
    err: Option<String>,
    rtt: AtomicU16,
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: AppConf = serde_json::from_value(conf.clone())?;
    Ok(())
}

pub fn validate_sink_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: AppConf = serde_json::from_value(conf).unwrap();

    Box::new(Websocket {
        _id: id,
        conf: Arc::new(conf),
        sources: DashMap::new(),
        sinks: DashMap::new(),
        err: None,
        rtt: AtomicU16::new(0),
    })
}

#[async_trait]
impl App for Websocket {
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
        let app_conf: Arc<AppConf> = Arc::new(serde_json::from_value(new_conf.clone())?);
        for mut source in self.sources.iter_mut() {
            source.update_app_conf(app_conf.clone()).await;
        }
        for mut sink in self.sinks.iter_mut() {
            sink.update_app_conf(app_conf.clone()).await;
        }
        self.conf = app_conf;
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
        let source_conf: SourceConf = serde_json::from_value(conf.clone())?;
        let source = Source::new(self.conf.clone(), source_conf).await;
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

    // TODO
    async fn get_source_rxs(
        &self,
        source_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>> {
        todo!()
        // match self.sinks.get(source_id) {
        //     Some(sink) => Ok(sink.mb_tx.clone()),
        //     None => Err(HaliaError::NotFound(source_id.to_owned())),
        // }
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let sink_conf: SinkConf = serde_json::from_value(conf.clone())?;
        let sink = Sink::new(self.conf.clone(), sink_conf).await;
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

    async fn get_sink_txs(
        &self,
        sink_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.get_txs(cnt)),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

fn connect_websocket(
    app_conf: &Arc<AppConf>,
    path: &String,
    headers: &Vec<(String, String)>,
) -> Request {
    match app_conf.ssl_enable {
        true => todo!(),
        false => {
            let mut request = format!("ws://{}:{}/{}", app_conf.host, app_conf.port, path)
                .into_client_request()
                .unwrap();
            for (key, value) in headers {
                request.headers_mut().insert(HOST, value.parse().unwrap());
            }
            request
        }
    }
}

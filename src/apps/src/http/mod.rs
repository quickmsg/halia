use std::sync::Arc;

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use futures::lock::BiLock;
use message::RuleMessageBatch;
use reqwest::{Certificate, Client, ClientBuilder, Identity, RequestBuilder};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use types::apps::http_client::{BasicAuth, HttpClientConf, SinkConf, SourceConf};
use utils::ErrorManager;

use crate::App;

mod sink;
mod source;

pub struct HttpClient {
    conf: Arc<HttpClientConf>,
    err: BiLock<Option<Arc<String>>>,
    sources: DashMap<String, Source>,
    sinks: DashMap<String, Sink>,
    device_err_tx: mpsc::UnboundedSender<Option<Arc<String>>>,
    stop_signal_tx: watch::Sender<()>,
    // join_handle: Option<JoinHandle<TaskLoop>>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: HttpClientConf = serde_json::from_value(conf).unwrap();

    let (device_err_tx, device_err_rx) = mpsc::unbounded_channel();

    let (err1, err2) = BiLock::new(None);
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());

    let task_loop = TaskLoop::new(id, err1, device_err_rx, stop_signal_rx);
    let _join_handle = task_loop.start();

    Box::new(HttpClient {
        conf: Arc::new(conf),
        err: err2,
        sources: DashMap::new(),
        sinks: DashMap::new(),
        device_err_tx,
        stop_signal_tx,
        // join_handle: Some(join_handle),
    })
}

struct TaskLoop {
    error_manager: ErrorManager,
    device_err_rx: mpsc::UnboundedReceiver<Option<Arc<String>>>,
    stop_signal_rx: watch::Receiver<()>,
}

impl TaskLoop {
    fn new(
        id: String,
        device_err: BiLock<Option<Arc<String>>>,
        device_err_rx: mpsc::UnboundedReceiver<Option<Arc<String>>>,
        stop_signal_rx: watch::Receiver<()>,
    ) -> Self {
        let error_manager =
            ErrorManager::new(utils::error_manager::ResourceType::App, id, device_err);
        Self {
            error_manager,
            device_err_rx,
            stop_signal_rx,
        }
    }

    fn start(mut self) -> JoinHandle<TaskLoop> {
        tokio::spawn(async move {
            loop {
                select! {
                    Some(err) = self.device_err_rx.recv() => {
                        self.handle_err(err).await;
                    }

                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }
                }
            }
        })
    }

    async fn handle_err(&mut self, err: Option<Arc<String>>) {
        match err {
            Some(err) => {
                self.error_manager.put_err(err.clone()).await;
            }
            None => {
                self.error_manager.set_ok().await;
            }
        }
    }
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let _: HttpClientConf = serde_json::from_value(conf.clone())?;
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
    async fn read_app_err(&self) -> Option<String> {
        match &(*self.err.lock().await) {
            Some(err) => Some((**err).clone()),
            None => None,
        }
    }

    async fn read_source_err(&self, source_id: &String) -> HaliaResult<Option<String>> {
        match self.sources.get(source_id) {
            Some(source) => Ok(source.read_err().await),
            // 错误提示 TODO
            None => Err(HaliaError::NotFound("source".to_owned())),
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

        self.stop_signal_tx.send(()).unwrap();
    }

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(
            source_id.clone(),
            self.conf.clone(),
            conf,
            self.device_err_tx.clone(),
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

    async fn get_source_rxs(
        &self,
        source_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>> {
        match self.sources.get_mut(source_id) {
            Some(mut source) => Ok(source.get_rxs(cnt).await),
            None => Err(HaliaError::NotFound(source_id.to_owned())),
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

fn build_http_client(http_client_conf: &HttpClientConf) -> Client {
    let mut builder = ClientBuilder::new();
    if let Some(ssl_conf) = &http_client_conf.ssl_conf {
        match ssl_conf.verify {
            true => builder = builder.danger_accept_invalid_certs(true),
            false => builder = builder.danger_accept_invalid_certs(false),
        }

        if let Some(ca_cert) = &ssl_conf.ca_cert {
            builder =
                builder.add_root_certificate(Certificate::from_pem(ca_cert.as_bytes()).unwrap());
        }

        match (&ssl_conf.client_cert, &ssl_conf.client_key) {
            (Some(client_cert), Some(client_key)) => {
                builder = builder.identity(
                    Identity::from_pem(format!("{}{}", client_cert, client_key).as_bytes())
                        .unwrap(),
                );
            }
            _ => {}
        }
    }
    builder.build().unwrap()
}

fn insert_headers(
    mut builder: RequestBuilder,
    headers_client: &Vec<(String, String)>,
    headers_item: &Vec<(String, String)>,
) -> RequestBuilder {
    for (k, v) in headers_client {
        builder = builder.header(k, v);
    }
    for (k, v) in headers_item {
        builder = builder.header(k, v);
    }

    builder
}

fn insert_query(
    mut builder: RequestBuilder,
    client_query_params: &Vec<(String, String)>,
    source_sink_query_params: &Vec<(String, String)>,
) -> RequestBuilder {
    builder = builder.query(client_query_params);
    builder.query(source_sink_query_params)
}

fn insert_basic_auth(builder: RequestBuilder, basic_auth: &Option<BasicAuth>) -> RequestBuilder {
    match basic_auth {
        Some(basic_auth) => {
            builder.basic_auth(basic_auth.username.clone(), basic_auth.password.clone())
        }
        None => builder,
    }
}

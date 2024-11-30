use std::sync::Arc;

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use futures::lock::BiLock;
use halia_derive::ResourceErr;
use influxdb::Client;
use message::RuleMessageBatch;
use sink::Sink;
use tokio::{
    select,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use types::apps::influxdb_v1::{InfluxdbConf, SinkConf};
use utils::ErrorManager;

use crate::App;

mod sink;

#[derive(ResourceErr)]
pub struct Influxdb {
    err: BiLock<Option<Arc<String>>>,
    sinks: DashMap<String, Sink>,
    influxdb_conf: Arc<InfluxdbConf>,
    app_err_tx: UnboundedSender<Option<Arc<String>>>,
    stop_signal_tx: watch::Sender<()>,
}

pub fn new(app_id: String, app_conf: serde_json::Value) -> Box<dyn App> {
    let influxdb_conf: InfluxdbConf = serde_json::from_value(app_conf).unwrap();
    let (err1, err2) = BiLock::new(None);
    let (app_err_tx, app_err_rx) = mpsc::unbounded_channel();
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());

    let task_loop = TaskLoop::new(app_id, err1, app_err_rx, stop_signal_rx);
    task_loop.start();

    Box::new(Influxdb {
        err: err2,
        sinks: DashMap::new(),
        influxdb_conf: Arc::new(influxdb_conf),
        app_err_tx,
        stop_signal_tx,
    })
}

struct TaskLoop {
    app_err_rx: mpsc::UnboundedReceiver<Option<Arc<String>>>,
    stop_signal_rx: watch::Receiver<()>,
    error_manager: ErrorManager,
}

impl TaskLoop {
    fn new(
        app_id: String,
        app_err: BiLock<Option<Arc<String>>>,
        app_err_rx: UnboundedReceiver<Option<Arc<String>>>,
        stop_signal_rx: watch::Receiver<()>,
    ) -> Self {
        let error_manager =
            ErrorManager::new(utils::error_manager::ResourceType::App, app_id, app_err);
        Self {
            app_err_rx,
            stop_signal_rx,
            error_manager,
        }
    }

    fn start(mut self) -> JoinHandle<TaskLoop> {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }
                    Some(err) = self.app_err_rx.recv() => {
                        self.handle_err(err).await;
                    }
                }
            }
        })
    }

    async fn handle_err(&mut self, err: Option<Arc<String>>) {
        match err {
            Some(err) => {
                self.error_manager.put_err(err).await;
            }
            None => {
                self.error_manager.set_ok().await;
            }
        }
    }
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: InfluxdbConf = serde_json::from_value(conf.clone())?;
    match conf.auth_method {
        types::apps::influxdb_v1::AuthMethod::None => {}
        types::apps::influxdb_v1::AuthMethod::Password => {
            if conf.auth_password.is_none() {
                return Err(HaliaError::Common("auth_password字段不能为空！".to_owned()));
            }
        }
        types::apps::influxdb_v1::AuthMethod::ApiToken => {
            if conf.auth_api_token.is_none() {
                return Err(HaliaError::Common(
                    "auth_api_token字段不能为空！".to_owned(),
                ));
            }
        }
    }

    Ok(())
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

#[async_trait]
impl App for Influxdb {
    async fn read_app_err(&self) -> Option<Arc<String>> {
        self.read_err().await
    }

    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: InfluxdbConf = serde_json::from_value(new_conf)?;
        self.influxdb_conf = Arc::new(new_conf);
        for mut sink in self.sinks.iter_mut() {
            sink.update_influxdb_client(self.influxdb_conf.clone())
                .await;
        }

        Ok(())
    }

    async fn stop(&mut self) {
        self.stop_signal_tx.send(()).unwrap();
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(
            sink_id.clone(),
            conf,
            self.influxdb_conf.clone(),
            self.app_err_tx.clone(),
        );
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: String,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: SinkConf = serde_json::from_value(new_conf)?;
        match self.sinks.get_mut(&sink_id) {
            Some(mut sink) => {
                sink.update_conf(new_conf).await;
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
    ) -> HaliaResult<Vec<UnboundedSender<RuleMessageBatch>>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.get_txs(cnt)),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

fn new_influxdb_client(influxdb_conf: &Arc<InfluxdbConf>, sink_conf: &SinkConf) -> Client {
    let schema = match &influxdb_conf.ssl_enable {
        true => "https",
        false => "http",
    };
    let mut client = Client::new(
        format!(
            "{}://{}:{}",
            schema, &influxdb_conf.host, influxdb_conf.port
        ),
        &sink_conf.database,
    );

    match influxdb_conf.auth_method {
        types::apps::influxdb_v1::AuthMethod::None => {}
        types::apps::influxdb_v1::AuthMethod::Password => {
            client = client.with_auth(
                influxdb_conf
                    .auth_password
                    .as_ref()
                    .unwrap()
                    .username
                    .clone(),
                influxdb_conf
                    .auth_password
                    .as_ref()
                    .unwrap()
                    .password
                    .clone(),
            )
        }
        types::apps::influxdb_v1::AuthMethod::ApiToken => {
            client = client.with_token(
                influxdb_conf
                    .auth_api_token
                    .as_ref()
                    .unwrap()
                    .api_token
                    .clone(),
            )
        }
    }

    client
}

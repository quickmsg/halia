use std::sync::Arc;

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use futures::lock::BiLock;
use halia_derive::ResourceErr;
use message::RuleMessageBatch;
use sink::Sink;
use tokio::{
    select,
    sync::{
        mpsc::{self, unbounded_channel, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use types::apps::influxdb_v2::{InfluxdbConf, SinkConf};
use utils::ErrorManager;

use crate::App;

mod sink;

#[derive(ResourceErr)]
pub struct Influxdb {
    err: BiLock<Option<Arc<String>>>,
    sinks: DashMap<String, Sink>,
    conf: Arc<InfluxdbConf>,
    app_err_tx: UnboundedSender<Option<Arc<String>>>,
    stop_signal_tx: watch::Sender<()>,
    // join_handle: Option<JoinHandle<TaskLoop>>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: InfluxdbConf = serde_json::from_value(conf).unwrap();

    let (app_err1, app_err2) = BiLock::new(None);
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());
    let (app_err_tx, app_err_rx) = unbounded_channel();
    let task_loop = TaskLoop::new(id.clone(), app_err1, app_err_rx, stop_signal_rx);
    let _join_handle = task_loop.start();
    Box::new(Influxdb {
        err: app_err2,
        sinks: DashMap::new(),
        conf: Arc::new(conf),
        app_err_tx,
        stop_signal_tx,
        // join_handle: Some(join_handle),
    })
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let _conf: InfluxdbConf = serde_json::from_value(conf.clone())?;
    Ok(())
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

struct TaskLoop {
    error_manager: ErrorManager,
    app_err_rx: mpsc::UnboundedReceiver<Option<Arc<String>>>,
    stop_signal_rx: watch::Receiver<()>,
}

impl TaskLoop {
    fn new(
        id: String,
        app_err: BiLock<Option<Arc<String>>>,
        app_err_rx: mpsc::UnboundedReceiver<Option<Arc<String>>>,
        stop_signal_rx: watch::Receiver<()>,
    ) -> Self {
        let error_manager = ErrorManager::new(utils::error_manager::ResourceType::App, id, app_err);
        Self {
            error_manager,
            app_err_rx,
            stop_signal_rx,
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
                // events::insert_connect_failed(
                //     types::events::ResourceType::App,
                //     &self.id,
                //     err.clone(),
                // )
                // .await;
                self.error_manager.put_err(err).await;
            }
            None => {
                // events::insert_connect_succeed(
                //     types::events::ResourceType::App,
                //     &join_handle_data.id,
                // )
                // .await;
                self.error_manager.set_ok().await;
            }
        }
    }
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
        self.conf = Arc::new(new_conf);
        for mut sink in self.sinks.iter_mut() {
            sink.update_influxdb_client(self.conf.clone()).await;
        }

        Ok(())
    }

    async fn stop(&mut self) {
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
        self.stop_signal_tx.send(()).unwrap();
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(
            sink_id.clone(),
            conf,
            self.conf.clone(),
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
    ) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.get_txs(cnt)),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

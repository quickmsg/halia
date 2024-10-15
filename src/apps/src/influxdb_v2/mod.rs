use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc,
};

use async_trait::async_trait;
use common::{
    constants::CHANNEL_SIZE,
    error::{HaliaError, HaliaResult},
};
use dashmap::DashMap;
use message::MessageBatch;
use sink::Sink;
use tokio::{
    select,
    sync::{mpsc, watch, RwLock},
};
use tracing::warn;
use types::apps::{
    influxdb_v2::{Conf, SinkConf},
    SearchAppsItemRunningInfo,
};

use crate::App;

mod sink;

pub struct Influxdb {
    err: Arc<RwLock<Option<String>>>,
    sinks: DashMap<String, Sink>,
    conf: Arc<Conf>,
    rtt: AtomicU16,
    app_err_tx: mpsc::Sender<String>,
    stop_signal_tx: watch::Sender<()>,
}

struct JoinHandleData {
    id: String,
    stop_signal_rx: watch::Receiver<()>,
    app_err_rx: mpsc::Receiver<String>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: Conf = serde_json::from_value(conf).unwrap();

    let (stop_signal_tx, stop_signal_rx) = watch::channel(());
    let (app_err_tx, app_err_rx) = mpsc::channel(CHANNEL_SIZE);
    let join_handle_data = JoinHandleData {
        id,
        stop_signal_rx,
        app_err_rx,
    };
    Influxdb::event_loop(join_handle_data);
    Box::new(Influxdb {
        err: Arc::new(RwLock::new(None)),
        sinks: DashMap::new(),
        conf: Arc::new(conf),
        rtt: AtomicU16::new(0),
        app_err_tx,
        stop_signal_tx,
    })
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let _conf: Conf = serde_json::from_value(conf.clone())?;
    Ok(())
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

impl Influxdb {
    fn event_loop(mut join_handle_data: JoinHandleData) {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        break;
                    }
                    err = join_handle_data.app_err_rx.recv() => {
                        if let Some(err) = err {
                            warn!("Influxdb app error: {}", err);
                        }
                    }
                }
            }
        });
    }
}

#[async_trait]
impl App for Influxdb {
    async fn read_running_info(&self) -> SearchAppsItemRunningInfo {
        SearchAppsItemRunningInfo {
            err: self.err.read().await.clone(),
            rtt: self.rtt.load(Ordering::SeqCst),
        }
    }

    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: Conf = serde_json::from_value(new_conf)?;
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
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(conf, self.conf.clone());
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

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

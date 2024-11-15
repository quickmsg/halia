use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use sink::Sink;
use tokio::{
    select,
    sync::{
        mpsc::{self, unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch, RwLock,
    },
};
use tracing::warn;
use types::apps::{
    influxdb_v2::{InfluxdbConf, SinkConf},
    SearchAppsItemRunningInfo,
};

use crate::{add_app_running_count, sub_app_running_count, App};

mod sink;

pub struct Influxdb {
    err: Arc<RwLock<Option<Arc<String>>>>,
    sinks: DashMap<String, Sink>,
    conf: Arc<InfluxdbConf>,
    rtt: AtomicU16,
    app_err_tx: UnboundedSender<Option<String>>,
    stop_signal_tx: watch::Sender<()>,
}

struct JoinHandleData {
    id: String,
    err: Arc<RwLock<Option<Arc<String>>>>,
    stop_signal_rx: watch::Receiver<()>,
    app_err_rx: UnboundedReceiver<Option<String>>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: InfluxdbConf = serde_json::from_value(conf).unwrap();

    let err = Arc::new(RwLock::new(None));
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());
    let (app_err_tx, app_err_rx) = unbounded_channel();
    let join_handle_data = JoinHandleData {
        id,
        err: err.clone(),
        stop_signal_rx,
        app_err_rx,
    };
    Influxdb::event_loop(join_handle_data);
    Box::new(Influxdb {
        err,
        sinks: DashMap::new(),
        conf: Arc::new(conf),
        rtt: AtomicU16::new(0),
        app_err_tx,
        stop_signal_tx,
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

impl Influxdb {
    fn event_loop(mut join_handle_data: JoinHandleData) {
        add_app_running_count();
        tokio::spawn(async move {
            let mut task_err = None;
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        break;
                    }
                    Some(err) = join_handle_data.app_err_rx.recv() => {
                        Self::handle_err(&join_handle_data, &mut task_err, err).await;
                    }
                }
            }
        });
    }

    async fn handle_err(
        join_handle_data: &JoinHandleData,
        task_err: &mut Option<Arc<String>>,
        err: Option<String>,
    ) {
        match err {
            Some(err) => {
                events::insert_connect_failed(
                    types::events::ResourceType::App,
                    &join_handle_data.id,
                    err.clone(),
                )
                .await;

                if task_err.is_none() {
                    sub_app_running_count();
                    if let Err(e) =
                        storage::app::update_err(&join_handle_data.id, types::Boolean::True).await
                    {
                        warn!("{}", e);
                    }
                    let err = Arc::new(err);
                    join_handle_data.err.write().await.replace(err.clone());
                    *task_err = Some(err);
                } else {
                    if *task_err.as_ref().unwrap().deref() != err {
                        let err = Arc::new(err);
                        join_handle_data.err.write().await.replace(err.clone());
                        *task_err = Some(err);
                    }
                }
            }
            None => {
                events::insert_connect_succeed(
                    types::events::ResourceType::App,
                    &join_handle_data.id,
                )
                .await;
                if task_err.is_some() {
                    if let Err(e) =
                        storage::app::update_err(&join_handle_data.id, types::Boolean::False).await
                    {
                        warn!("{}", e);
                    }
                    *task_err = None;
                    *join_handle_data.err.write().await = None;
                }
            }
        }
    }
}

#[async_trait]
impl App for Influxdb {
    async fn read_running_info(&self) -> SearchAppsItemRunningInfo {
        let err = match self.err.read().await.as_ref() {
            Some(err) => Some(err.as_ref().clone()),
            None => None,
        };
        SearchAppsItemRunningInfo {
            err,
            rtt: self.rtt.load(Ordering::SeqCst),
        }
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
        let sink = Sink::new(conf, self.conf.clone(), self.app_err_tx.clone());
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

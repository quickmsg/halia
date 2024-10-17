use std::{
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use rskafka::client::{Client, ClientBuilder};
use sink::Sink;
use tokio::{
    select,
    sync::{mpsc, watch, RwLock},
    task::JoinHandle,
    time,
};
use tracing::debug;
use types::apps::{
    kafka::{Conf, SinkConf},
    SearchAppsItemRunningInfo,
};

use crate::{add_app_running_count, sub_app_running_count, App};

mod sink;

pub struct Kafka {
    err: Arc<RwLock<Option<Arc<String>>>>,
    stop_signal_tx: watch::Sender<()>,

    kafka_client: Arc<RwLock<Option<Client>>>,

    sinks: Arc<DashMap<String, Sink>>,
    kafka_err_tx: mpsc::Sender<String>,
    jh: Option<JoinHandle<JoinHandleData>>,
    rtt: AtomicU16,
}

struct JoinHandleData {
    pub id: String,
    pub conf: Conf,
    pub err: Arc<RwLock<Option<Arc<String>>>>,
    pub kafka_client: Arc<RwLock<Option<Client>>>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub kafka_err_rx: mpsc::Receiver<String>,
    pub sinks: Arc<DashMap<String, Sink>>,
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
    let conf: Conf = serde_json::from_value(conf).unwrap();
    let kafka_client = Arc::new(RwLock::new(None));
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());

    let sinks = Arc::new(DashMap::new());
    let (kafka_err_tx, kafka_err_rx) = mpsc::channel(1);

    let err = Arc::new(RwLock::new(None));

    let jhd = JoinHandleData {
        id,
        conf,
        err: err.clone(),
        kafka_client: kafka_client.clone(),
        stop_signal_rx,
        kafka_err_rx,
        sinks: sinks.clone(),
    };

    let jh = Kafka::event_loop(jhd);

    Box::new(Kafka {
        err,
        sinks,
        kafka_client,
        stop_signal_tx,
        kafka_err_tx,
        jh: Some(jh),
        rtt: AtomicU16::new(0),
    })
}

impl Kafka {
    async fn connect_loop(join_handle_data: &mut JoinHandleData) {
        let mut bootstrap_brokers = vec![];
        for (host, port) in &join_handle_data.conf.bootstrap_brokers {
            bootstrap_brokers.push(format!("{}:{}", host, port));
        }
        let reconnect = join_handle_data.conf.reconnect;
        let mut task_err: Option<Arc<String>> = None;
        loop {
            match ClientBuilder::new(bootstrap_brokers.clone()).build().await {
                Ok(client) => {
                    add_app_running_count();
                    *join_handle_data.kafka_client.write().await = Some(client);
                    events::insert_connect_succeed(
                        types::events::ResourceType::App,
                        &join_handle_data.id,
                    )
                    .await;
                    return;
                }
                Err(e) => {
                    events::insert_connect_failed(
                        types::events::ResourceType::App,
                        &join_handle_data.id,
                        e.to_string(),
                    )
                    .await;

                    match &task_err {
                        Some(inner_task_err) => {
                            sub_app_running_count();
                            if **inner_task_err != e.to_string() {
                                let err = Arc::new(e.to_string());
                                task_err = Some(err.clone());
                                join_handle_data.err.write().await.replace(err);
                            }
                        }
                        None => {
                            let _ = storage::app::update_err(&join_handle_data.id, true).await;
                            let err = Arc::new(e.to_string());
                            task_err = Some(err.clone());
                            join_handle_data.err.write().await.replace(err);
                        }
                    }

                    let sleep = time::sleep(Duration::from_secs(reconnect));
                    tokio::pin!(sleep);
                    select! {
                        _ = join_handle_data.stop_signal_rx.changed() => {
                            return;
                        }

                        _ = &mut sleep => {}
                    }
                }
            }
        }
    }

    fn event_loop(mut jhd: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            Self::connect_loop(&mut jhd).await;
            Self::handle_connect_status_changed(&jhd.kafka_client, &jhd.sinks).await;

            loop {
                select! {
                    _ = jhd.stop_signal_rx.changed() => {
                        return jhd;
                    }

                    e = jhd.kafka_err_rx.recv() => {
                        debug!("Kafka error received, {:?}", e);
                        Self::connect_loop(&mut jhd).await;
                        Self::handle_connect_status_changed(&jhd.kafka_client, &jhd.sinks).await;
                    }
                }
            }
        })
    }

    async fn handle_connect_status_changed(
        kafka_client: &Arc<RwLock<Option<Client>>>,
        sinks: &Arc<DashMap<String, Sink>>,
    ) {
        let kafka_client = kafka_client.read().await;
        for mut sink in sinks.iter_mut() {
            sink.update_kafka_client(kafka_client.as_ref()).await;
        }
    }
}

#[async_trait]
impl App for Kafka {
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
        let new_conf: Conf = serde_json::from_value(new_conf)?;
        self.stop_signal_tx.send(()).unwrap();
        let mut jhd = self.jh.take().unwrap().await.unwrap();
        jhd.conf = new_conf;
        Self::event_loop(jhd);
        Ok(())
    }

    async fn stop(&mut self) {
        self.stop_signal_tx.send(()).unwrap();

        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf.clone())?;
        let sink = Sink::new(
            self.kafka_client.read().await.as_ref(),
            self.kafka_err_tx.clone(),
            conf,
        )
        .await;
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

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

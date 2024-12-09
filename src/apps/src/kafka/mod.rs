use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use futures::lock::BiLock;
use halia_derive::ResourceErr;
use message::RuleMessageBatch;
use rskafka::client::{Client, ClientBuilder};
use sink::Sink;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch, RwLock,
    },
    task::JoinHandle,
    time,
};
use tracing::debug;
use types::apps::kafka::{Conf, SinkConf};
use utils::ErrorManager;

use crate::App;

mod sink;

#[derive(ResourceErr)]
pub struct Kafka {
    err: BiLock<Option<Arc<String>>>,
    stop_signal_tx: watch::Sender<()>,

    kafka_client: Arc<RwLock<Option<Client>>>,

    sinks: Arc<DashMap<String, Sink>>,
    app_err_tx: UnboundedSender<Option<Arc<String>>>,
    join_handle: Option<JoinHandle<TaskLoop>>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: Conf = serde_json::from_value(conf).unwrap();
    let kafka_client = Arc::new(RwLock::new(None));
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());

    let sinks = Arc::new(DashMap::new());
    let (app_err_tx, app_err_rx) = unbounded_channel();

    let (err1, err2) = BiLock::new(None);

    // let jhd = JoinHandleData {
    //     id,
    //     conf,
    //     err: err1,
    //     kafka_client: kafka_client.clone(),
    //     stop_signal_rx,
    //     kafka_err_rx,
    //     sinks: sinks.clone(),
    // };

    // let jh = Kafka::event_loop(jhd);

    Box::new(Kafka {
        err: err2,
        sinks,
        kafka_client,
        stop_signal_tx,
        app_err_tx,
        join_handle: todo!(),
    })
}

struct TaskLoop {
    app_id: String,
    app_conf: Conf,
    kafka_client: Arc<RwLock<Option<Client>>>,
    stop_signal_rx: watch::Receiver<()>,
    app_err_rx: UnboundedReceiver<Option<Arc<String>>>,
    sinks: Arc<DashMap<String, Sink>>,
    error_manager: ErrorManager,
}

impl TaskLoop {
    fn new(
        app_id: String,
        app_conf: Conf,
        app_err: BiLock<Option<Arc<String>>>,
        app_err_rx: UnboundedReceiver<Option<Arc<String>>>,
    ) -> Self {
        let error_manager =
            ErrorManager::new(utils::error_manager::ResourceType::App, app_id, app_err);
        todo!()
    }

    fn start(mut self) -> JoinHandle<Self> {
        tokio::spawn(async move {
            self.connect_loop().await;
            // Self::handle_connect_status_changed(&jhd.kafka_client, &jhd.sinks).await;

            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    e = self.app_err_rx.recv() => {
                        debug!("Kafka error received, {:?}", e);
                        self.connect_loop().await;
                        // Self::handle_connect_status_changed(&jhd.kafka_client, &jhd.sinks).await;
                    }
                }
            }
        })
    }

    async fn connect_loop(&mut self) {
        let mut bootstrap_brokers = vec![];
        for (host, port) in &self.app_conf.bootstrap_brokers {
            bootstrap_brokers.push(format!("{}:{}", host, port));
        }
        let reconnect = self.app_conf.reconnect;
        loop {
            match ClientBuilder::new(bootstrap_brokers.clone()).build().await {
                Ok(client) => {
                    *self.kafka_client.write().await = Some(client);
                    events::insert_connect_succeed(types::events::ResourceType::App, &self.app_id)
                        .await;
                    return;
                }
                Err(e) => {
                    events::insert_connect_failed(
                        types::events::ResourceType::App,
                        &self.app_id,
                        e.to_string(),
                    )
                    .await;

                    let err = Arc::new(e.to_string());
                    let status_changed = self.error_manager.set_err(err).await;
                    if status_changed {}

                    let sleep = time::sleep(Duration::from_secs(reconnect));
                    tokio::pin!(sleep);
                    select! {
                        _ = self.stop_signal_rx.changed() => {
                            return;
                        }

                        _ = &mut sleep => {}
                    }
                }
            }
        }
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

pub fn validate_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

#[async_trait]
impl App for Kafka {
    async fn read_app_err(&self) -> Option<Arc<String>> {
        self.read_err().await
    }

    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: Conf = serde_json::from_value(new_conf)?;
        self.stop_signal_tx.send(()).unwrap();
        let mut task_loop = self.join_handle.take().unwrap().await.unwrap();
        task_loop.app_conf = new_conf;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
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
            sink_id.clone(),
            conf,
            self.kafka_client.read().await.as_ref(),
            self.app_err_tx.clone(),
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

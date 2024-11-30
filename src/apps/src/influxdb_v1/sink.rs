use std::sync::Arc;

use common::{
    error::HaliaResult,
    get_dynamic_value_from_json,
    sink_message_retain::{self, SinkMessageRetain},
};
use futures::lock::BiLock;
use influxdb::{Client, InfluxDbWriteable as _, Timestamp, Type};
use message::RuleMessageBatch;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tracing::debug;
use types::apps::influxdb_v1::{InfluxdbConf, SinkConf};
use utils::ErrorManager;

use super::new_influxdb_client;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<TaskLoop>>,
    err: BiLock<Option<Arc<String>>>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn new(
        sink_id: String,
        sink_conf: SinkConf,
        influxdb_conf: Arc<InfluxdbConf>,
        app_err_tx: UnboundedSender<Option<Arc<String>>>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();
        let (err1, err2) = BiLock::new(None);

        let task_loop = TaskLoop::new(
            sink_id,
            sink_conf,
            err1,
            influxdb_conf,
            app_err_tx,
            stop_signal_rx,
            mb_rx,
        );
        let join_handle = task_loop.start();

        Sink {
            stop_signal_tx,
            mb_tx,
            err: err2,
            join_handle: Some(join_handle),
        }
    }

    pub async fn stop(&mut self) -> TaskLoop {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_conf(&mut self, sink_conf: SinkConf) {
        let mut task_loop = self.stop().await;
        task_loop.sink_conf = sink_conf;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
    }

    pub async fn update_influxdb_client(&mut self, influxdb_conf: Arc<InfluxdbConf>) {
        let mut task_loop = self.stop().await;
        task_loop.influxdb_conf = influxdb_conf;
        let join_handle = task_loop.start();
        self.join_handle = Some(join_handle);
    }

    pub fn get_txs(&self, cnt: usize) -> Vec<UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];
        for _ in 0..cnt {
            txs.push(self.mb_tx.clone());
        }
        txs
    }
}

pub struct TaskLoop {
    sink_conf: SinkConf,
    influxdb_conf: Arc<InfluxdbConf>,
    app_err_tx: UnboundedSender<Option<Arc<String>>>,
    message_retainer: Box<dyn SinkMessageRetain>,
    stop_signal_rx: watch::Receiver<()>,
    mb_rx: UnboundedReceiver<RuleMessageBatch>,
    error_manager: ErrorManager,
}

impl TaskLoop {
    fn new(
        sink_id: String,
        sink_conf: SinkConf,
        sink_err: BiLock<Option<Arc<String>>>,
        influxdb_conf: Arc<InfluxdbConf>,
        app_err_tx: UnboundedSender<Option<Arc<String>>>,
        stop_signal_rx: watch::Receiver<()>,
        mb_rx: UnboundedReceiver<RuleMessageBatch>,
    ) -> Self {
        let message_retainer = sink_message_retain::new(&sink_conf.message_retain);
        let error_manager = ErrorManager::new(
            utils::error_manager::ResourceType::AppSink,
            sink_id,
            sink_err,
        );
        Self {
            sink_conf,
            influxdb_conf,
            app_err_tx,
            message_retainer,
            stop_signal_rx,
            mb_rx,
            error_manager,
        }
    }

    fn start(mut self) -> JoinHandle<Self> {
        let influxdb_client = new_influxdb_client(&self.influxdb_conf, &self.sink_conf);

        tokio::spawn(async move {
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    Some(rmb) = self.mb_rx.recv() => {
                        self.send_msg_to_influxdb(&influxdb_client, rmb).await;
                    }
                }
            }
        })
    }

    async fn send_msg_to_influxdb(&mut self, influxdb_client: &Client, rmb: RuleMessageBatch) {
        debug!("{:?}", rmb);
        let mb = rmb.take_mb();
        let mut querys = vec![];
        let ts = chrono::Local::now().timestamp() as u128;
        let timestamp = match &self.sink_conf.precision {
            types::apps::influxdb_v1::Precision::Nanoseconds => Timestamp::Nanoseconds(ts),
            types::apps::influxdb_v1::Precision::Microseconds => Timestamp::Microseconds(ts),
            types::apps::influxdb_v1::Precision::Milliseconds => Timestamp::Milliseconds(ts),
            types::apps::influxdb_v1::Precision::Seconds => Timestamp::Seconds(ts),
            types::apps::influxdb_v1::Precision::Minutes => Timestamp::Minutes(ts),
            types::apps::influxdb_v1::Precision::Hours => Timestamp::Hours(ts),
        };
        for msg in mb.get_messages() {
            let mut query = timestamp.into_query(&self.sink_conf.mesaurement);
            for (field, field_value) in &self.sink_conf.fields {
                let value = match get_dynamic_value_from_json(field_value) {
                    common::DynamicValue::Const(value) => value,
                    common::DynamicValue::Field(s) => match msg.get(&s) {
                        Some(value) => value.clone().into(),
                        None => serde_json::Value::Null,
                    },
                };

                match value {
                    serde_json::Value::Bool(b) => query = query.add_field(field, Type::Boolean(b)),
                    serde_json::Value::Number(number) => {
                        if let Some(i) = number.as_i64() {
                            query = query.add_field(field, Type::SignedInteger(i));
                        } else if let Some(u) = number.as_u64() {
                            query = query.add_field(field, Type::UnsignedInteger(u));
                        } else if let Some(f) = number.as_f64() {
                            query = query.add_field(field, Type::Float(f));
                        }
                    }
                    serde_json::Value::String(s) => {
                        query = query.add_field(field, Type::Text(s));
                    }
                    _ => {}
                }
            }

            if let Some(tags) = &self.sink_conf.tags {
                for (tag, tag_value) in tags {
                    let value = match get_dynamic_value_from_json(tag_value) {
                        common::DynamicValue::Const(value) => value,
                        common::DynamicValue::Field(s) => match msg.get(&s) {
                            Some(value) => value.clone().into(),
                            None => serde_json::Value::Null,
                        },
                    };

                    match value {
                        serde_json::Value::Bool(b) => query = query.add_tag(tag, Type::Boolean(b)),
                        serde_json::Value::Number(number) => {
                            if let Some(i) = number.as_i64() {
                                query = query.add_tag(tag, Type::SignedInteger(i));
                            } else if let Some(u) = number.as_u64() {
                                query = query.add_tag(tag, Type::UnsignedInteger(u));
                            } else if let Some(f) = number.as_f64() {
                                query = query.add_tag(tag, Type::Float(f));
                            }
                        }
                        serde_json::Value::String(s) => {
                            query = query.add_tag(tag, Type::Text(s));
                        }
                        _ => {}
                    }
                }
            }
            querys.push(query);
        }
        if let Err(e) = influxdb_client.query(querys).await {
            match e {
                influxdb::Error::InvalidQueryError { error } => todo!(),
                influxdb::Error::UrlConstructionError { error } => todo!(),
                influxdb::Error::ProtocolError { error } => todo!(),
                influxdb::Error::DeserializationError { error } => todo!(),
                influxdb::Error::DatabaseError { error } => {
                    // TODO database error 说明已连接
                    let err = Arc::new(error);
                    self.error_manager.put_err(err.clone()).await;
                }
                influxdb::Error::AuthenticationError => todo!(),
                influxdb::Error::AuthorizationError => todo!(),
                influxdb::Error::ConnectionError { error } => {
                    let err = Arc::new(error);
                    let status_changed = self.error_manager.put_err(err.clone()).await;
                    if status_changed {
                        let _ = self.app_err_tx.send(Some(err));
                    }
                }
            }
        }
    }
}

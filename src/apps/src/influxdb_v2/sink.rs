use std::sync::Arc;

use common::{
    error::HaliaResult,
    get_dynamic_value_from_json,
    sink_message_retain::{self, SinkMessageRetain},
};
use futures::{lock::BiLock, stream};
use influxdb2::{
    api::write::TimestampPrecision,
    models::{DataPoint, FieldValue},
    Client,
};
use message::RuleMessageBatch;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tracing::{debug, warn};
use types::apps::influxdb_v2::{InfluxdbConf, SinkConf};
use utils::ErrorManager;

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
        let (sink_err1, sink_err2) = BiLock::new(None);

        let task_loop = TaskLoop::new(
            sink_id,
            sink_conf,
            sink_err1,
            influxdb_conf,
            app_err_tx,
            stop_signal_rx,
            mb_rx,
        );
        let join_handle = task_loop.start();

        Sink {
            stop_signal_tx,
            mb_tx,
            err: sink_err2,
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
            sink_id.clone(),
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
        let influxdb_client = new_influxdb_client(&self.influxdb_conf);

        let timestamp_precision = match &self.sink_conf.precision {
            types::apps::influxdb_v2::Precision::Seconds => TimestampPrecision::Seconds,
            types::apps::influxdb_v2::Precision::Milliseconds => TimestampPrecision::Milliseconds,
            types::apps::influxdb_v2::Precision::Microseconds => TimestampPrecision::Microseconds,
            types::apps::influxdb_v2::Precision::Nanoseconds => TimestampPrecision::Nanoseconds,
        };

        tokio::spawn(async move {
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    Some(mb) = self.mb_rx.recv() => {
                        // if !app_err {
                       self.send_msg_to_influxdb(&influxdb_client, timestamp_precision.clone(), mb).await;
                        // } else {
                        //     join_handle_data.message_retainer.push(mb);
                        // }
                    }
                }
            }
        })
    }

    async fn send_msg_to_influxdb(
        &mut self,
        influxdb_client: &Client,
        timestamp_precision: TimestampPrecision,
        rmb: RuleMessageBatch,
    ) {
        let mb = rmb.take_mb();
        let mut data_points = Vec::with_capacity(mb.len());
        for msg in mb.get_messages() {
            let mut data_point_builder = DataPoint::builder(&self.sink_conf.mesaurement);
            for (field, field_value) in &self.sink_conf.fields {
                let value = match get_dynamic_value_from_json(field_value) {
                    common::DynamicValue::Const(value) => value,
                    common::DynamicValue::Field(s) => match msg.get(&s) {
                        Some(value) => value.clone().into(),
                        None => serde_json::Value::Null,
                    },
                };

                let value = match value {
                    serde_json::Value::Bool(b) => FieldValue::Bool(b),
                    serde_json::Value::Number(number) => {
                        if number.is_f64() {
                            FieldValue::F64(number.as_f64().unwrap())
                        } else {
                            FieldValue::I64(number.as_i64().unwrap())
                        }
                    }
                    serde_json::Value::String(s) => FieldValue::String(s),
                    serde_json::Value::Null
                    | serde_json::Value::Array(_)
                    | serde_json::Value::Object(_) => {
                        warn!("Unsupported field value: {:?}", value);
                        break;
                    }
                };

                debug!("{:?}", value);

                data_point_builder = data_point_builder.field(field, value);
            }

            match data_point_builder.build() {
                Ok(data_point) => data_points.push(data_point),
                Err(e) => warn!("Failed to build point: {}", e),
            }
        }

        match influxdb_client
            .write_with_precision(
                &self.sink_conf.bucket,
                stream::iter(data_points),
                timestamp_precision,
            )
            .await
        {
            Ok(_) => {
                let status_changed = self.error_manager.set_ok().await;
                if status_changed {
                    self.app_err_tx.send(None).unwrap();
                }
            }
            Err(e) => {
                self.message_retainer.push(mb);
                match e {
                    influxdb2::RequestError::ReqwestProcessing { source } => {
                        let err = Arc::new(source.to_string());
                        let status_changed = self.error_manager.put_err(err.clone()).await;
                        if status_changed {
                            self.app_err_tx.send(Some(err)).unwrap();
                        }
                    }
                    influxdb2::RequestError::Http { status, text } => {
                        // TODO sink err
                        debug!("Failed to write to influxdb http: {}, {}", status, text);
                    }
                    // TODO sink err
                    influxdb2::RequestError::Serializing { source } => {
                        debug!("{}", source);
                    }
                    // TODO sink err
                    influxdb2::RequestError::Deserializing { text } => {
                        debug!("{}", text);
                    }
                }
            }
        }
    }
}

fn new_influxdb_client(influxdb_conf: &Arc<InfluxdbConf>) -> Client {
    Client::new(
        format!("http://{}:{}", &influxdb_conf.host, influxdb_conf.port),
        &influxdb_conf.org,
        &influxdb_conf.api_token,
    )
}
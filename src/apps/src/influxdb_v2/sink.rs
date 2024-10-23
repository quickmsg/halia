use std::sync::Arc;

use common::{
    error::HaliaResult,
    get_dynamic_value_from_json,
    sink_message_retain::{self, SinkMessageRetain},
};
use futures::stream;
use influxdb2::{
    api::write::TimestampPrecision,
    models::{DataPoint, FieldValue},
    Client,
};
use message::MessageBatch;
use tokio::{
    select,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::{debug, warn};
use types::apps::influxdb_v2::{Conf, SinkConf};

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

pub struct JoinHandleData {
    pub app_err_tx: mpsc::Sender<Option<String>>,
    pub conf: SinkConf,
    pub influxdb_conf: Arc<Conf>,
    pub message_retainer: Box<dyn SinkMessageRetain>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: mpsc::Receiver<MessageBatch>,
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn new(
        conf: SinkConf,
        influxdb_conf: Arc<Conf>,
        app_err_tx: mpsc::Sender<Option<String>>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let join_handle_data = JoinHandleData {
            conf,
            influxdb_conf,
            message_retainer,
            stop_signal_rx,
            mb_rx,
            app_err_tx,
        };
        let join_handle = Self::event_loop(join_handle_data);

        Sink {
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
        }
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        let influxdb_client =
            new_influxdb_client(&join_handle_data.influxdb_conf, &join_handle_data.conf);

        let timestamp_precision = match &join_handle_data.conf.precision {
            types::apps::influxdb_v2::Precision::Seconds => TimestampPrecision::Seconds,
            types::apps::influxdb_v2::Precision::Milliseconds => TimestampPrecision::Milliseconds,
            types::apps::influxdb_v2::Precision::Microseconds => TimestampPrecision::Microseconds,
            types::apps::influxdb_v2::Precision::Nanoseconds => TimestampPrecision::Nanoseconds,
        };

        tokio::spawn(async move {
            let mut err = None;
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    Some(mb) = join_handle_data.mb_rx.recv() => {
                        // if !app_err {
                        Self::send_msg_to_influxdb(&join_handle_data.app_err_tx, &mut err, &influxdb_client, &join_handle_data.conf, timestamp_precision.clone(), mb, &mut join_handle_data.message_retainer).await;
                        // } else {
                        //     join_handle_data.message_retainer.push(mb);
                        // }
                    }
                }
            }
        })
    }

    async fn send_msg_to_influxdb(
        app_err_tx: &mpsc::Sender<Option<String>>,
        err: &mut Option<String>,
        influxdb_client: &Client,
        conf: &SinkConf,
        timestamp_precision: TimestampPrecision,
        mb: MessageBatch,
        message_retainer: &mut Box<dyn SinkMessageRetain>,
    ) {
        let mut data_points = Vec::with_capacity(mb.len());
        for msg in mb.get_messages() {
            let mut data_point_builder = DataPoint::builder(&conf.mesaurement);
            for (field, field_value) in &conf.fields {
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
            .write_with_precision(&conf.bucket, stream::iter(data_points), timestamp_precision)
            .await
        {
            Ok(_) => match err {
                Some(_) => {
                    app_err_tx.send(None).await.unwrap();
                    *err = None;
                }
                None => {}
            },
            Err(e) => {
                match e {
                    influxdb2::RequestError::ReqwestProcessing { source } => {
                        message_retainer.push(mb);
                        match err {
                            Some(err) => {
                                if *err != source.to_string() {
                                    app_err_tx.send(Some(source.to_string())).await.unwrap();
                                    *err = source.to_string();
                                }
                            }
                            None => {
                                app_err_tx.send(Some(source.to_string())).await.unwrap();
                                *err = Some(source.to_string());
                            }
                        };
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

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_conf(&mut self, conf: SinkConf) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = conf;
        self.join_handle = Some(Self::event_loop(join_handle_data));
    }

    pub async fn update_influxdb_client(&mut self, influxdb_conf: Arc<Conf>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.influxdb_conf = influxdb_conf;
        self.join_handle = Some(Self::event_loop(join_handle_data));
    }
}

fn new_influxdb_client(influxdb_conf: &Arc<Conf>, sink_conf: &SinkConf) -> Client {
    Client::new(
        format!("http://{}:{}", &influxdb_conf.host, influxdb_conf.port),
        &sink_conf.org,
        &sink_conf.api_token,
    )
}

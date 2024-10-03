use std::sync::Arc;

use common::{
    error::HaliaResult,
    sink_message_retain::{self, SinkMessageRetain},
};
use futures::stream;
use influxdb::{InfluxDbWriteable as _, Timestamp};
use influxdb2::models::DataPoint;
use message::MessageBatch;
use tokio::{
    select,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::debug;
use types::apps::influxdb::{InfluxdbConf, SinkConf};

use super::{new_influxdb_client, InfluxdbClient};

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

pub struct JoinHandleData {
    pub conf: SinkConf,
    pub influxdb_conf: Arc<InfluxdbConf>,
    pub message_retainer: Box<dyn SinkMessageRetain>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: mpsc::Receiver<MessageBatch>,
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn new(conf: SinkConf, influxdb_conf: Arc<InfluxdbConf>) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let join_handle_data = JoinHandleData {
            conf,
            influxdb_conf,
            message_retainer,
            stop_signal_rx,
            mb_rx,
        };
        let join_handle = Self::event_loop(join_handle_data);

        Sink {
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
        }
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        let influxdb_client = new_influxdb_client(&join_handle_data.influxdb_conf);

        tokio::spawn(async move {
            let app_err = false;
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    Some(mb) = join_handle_data.mb_rx.recv() => {
                        if !app_err {
                            Self::send_msg_to_influxdb(&influxdb_client, &join_handle_data.influxdb_conf, &join_handle_data.conf, mb).await;
                        } else {
                            join_handle_data.message_retainer.push(mb);
                        }
                    }
                }
            }
        })
    }

    async fn send_msg_to_influxdb(
        influxdb_client: &InfluxdbClient,
        influxdb_conf: &Arc<InfluxdbConf>,
        _conf: &SinkConf,
        mb: MessageBatch,
    ) {
        match influxdb_client {
            InfluxdbClient::V1(client) => {
                let mut querys = vec![];
                for msg in mb.get_messages() {
                    let query = Timestamp::Nanoseconds(0)
                        .into_query("measurement")
                        .add_tag("xx", "xx")
                        .add_field("field1", 5);
                    querys.push(query);
                }
                let query = Timestamp::Nanoseconds(0)
                    .into_query("measurement")
                    .add_tag("xx", "xx")
                    .add_field("field1", 5);

                let results = client.query(vec![query]).await.unwrap();
                debug!("InfluxDB results: {:?}", results);
            }
            InfluxdbClient::V2(client) => {
                let mut points = vec![];
                for msg in mb.get_messages() {
                    let point = DataPoint::builder("cpu")
                        .tag("host", "server01")
                        .field("usage", 0.5)
                        .build()
                        .unwrap();
                    points.push(point);
                }
                client
                    .write(
                        &influxdb_conf.v2.as_ref().unwrap().bucket,
                        stream::iter(points),
                    )
                    .await
                    .unwrap();
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

    pub async fn update_influxdb_client(&mut self, influxdb_conf: Arc<InfluxdbConf>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.influxdb_conf = influxdb_conf;
        self.join_handle = Some(Self::event_loop(join_handle_data));
    }
}

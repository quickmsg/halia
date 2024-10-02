use std::sync::Arc;

use common::{
    error::HaliaResult,
    sink_message_retain::{self, SinkMessageRetain},
};
use influxdb::{Client, InfluxDbWriteable as _, Timestamp};
use message::MessageBatch;
use tokio::{
    select,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::debug;
use types::apps::influxdb::{InfluxdbConf, SinkConf};

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<
        JoinHandle<(
            SinkConf,
            Arc<InfluxdbConf>,
            Box<dyn SinkMessageRetain>,
            watch::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
        )>,
    >,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn new(conf: SinkConf, influxdb_conf: Arc<InfluxdbConf>) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = mpsc::channel(16);
        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let join_handle =
            Self::event_loop(conf, influxdb_conf, message_retainer, stop_signal_rx, mb_rx);
        Sink {
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
        }
    }

    fn event_loop(
        conf: SinkConf,
        influxdb_conf: Arc<InfluxdbConf>,
        message_retainer: Box<dyn sink_message_retain::SinkMessageRetain>,
        mut stop_signal_rx: watch::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) -> JoinHandle<(
        SinkConf,
        Arc<InfluxdbConf>,
        Box<dyn SinkMessageRetain>,
        watch::Receiver<()>,
        mpsc::Receiver<MessageBatch>,
    )> {
        let influxdb_client = Client::new(&influxdb_conf.url, &influxdb_conf.db);
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return (conf, influxdb_conf, message_retainer, stop_signal_rx, mb_rx);
                    }

                    Some(mb) = mb_rx.recv() => {
                        Self::send_msg_to_influxdb(&influxdb_client, mb, &message_retainer).await;
                    }
                }
            }
        })
    }

    async fn send_msg_to_influxdb(
        influxdb_client: &Client,
        _mb: MessageBatch,
        message_retainer: &Box<dyn sink_message_retain::SinkMessageRetain>,
    ) {
        let query = Timestamp::Nanoseconds(0)
            .into_query("measurement")
            .add_field("field1", 5);

        // TODO v1 v2
        let results = influxdb_client.query(vec![query]).await.unwrap();
        debug!("InfluxDB results: {:?}", results);
    }

    pub async fn stop(
        &mut self,
    ) -> (
        SinkConf,
        Arc<InfluxdbConf>,
        Box<dyn SinkMessageRetain>,
        watch::Receiver<()>,
        mpsc::Receiver<MessageBatch>,
    ) {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_conf(&mut self, conf: SinkConf) {
        let (_, influxdb_conf, message_retainer, stop_signal_rx, mb_rx) = self.stop().await;
        self.join_handle = Some(Self::event_loop(
            conf,
            influxdb_conf,
            message_retainer,
            stop_signal_rx,
            mb_rx,
        ));
    }

    pub async fn update_influxdb_client(&mut self, influxdb_conf: Arc<InfluxdbConf>) {
        let (conf, _, message_retainer, stop_signal_rx, mb_rx) = self.stop().await;
        self.join_handle = Some(Self::event_loop(
            conf,
            influxdb_conf,
            message_retainer,
            stop_signal_rx,
            mb_rx,
        ));
    }
}
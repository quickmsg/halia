use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    sink_message_retain::{self, SinkMessageRetain},
};
use tracing::warn;
use message::RuleMessageBatch;
use rumqttc::{valid_topic, AsyncClient};
use schema::Encoder;
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use types::apps::mqtt_client_v311::SinkConf;

use super::transfer_qos;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub struct JoinHandleData {
    pub conf: SinkConf,
    pub encoder: Box<dyn Encoder>,
    pub message_retainer: Box<dyn SinkMessageRetain>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: UnboundedReceiver<RuleMessageBatch>,
    pub mqtt_client: Arc<AsyncClient>,
    pub _app_err_rx: broadcast::Receiver<bool>,
}

impl Sink {
    pub fn validate_conf(conf: &SinkConf) -> HaliaResult<()> {
        if !valid_topic(&conf.topic) {
            return Err(HaliaError::Common("topic不合法！".to_owned()));
        }

        Ok(())
    }

    pub async fn new(
        conf: SinkConf,
        mqtt_client: Arc<AsyncClient>,
        app_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();
        let encoder = schema::new_encoder(&conf.encode_type, &conf.schema_id)
            .await
            .unwrap();

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let join_handle_data = JoinHandleData {
            conf,
            encoder,
            message_retainer,
            stop_signal_rx,
            mb_rx,
            mqtt_client,
            _app_err_rx: app_err_rx,
        };

        let join_handle = Self::event_loop(join_handle_data);

        Self {
            mb_tx,
            stop_signal_tx,
            join_handle: Some(join_handle),
        }
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        let mut err = false;
        let qos = transfer_qos(&join_handle_data.conf.qos);
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    Some(mb) = join_handle_data.mb_rx.recv() => {
                        let mb = mb.take_mb();
                        if !err {
                            match join_handle_data.encoder.encode(mb) {
                                Ok(data) => {
                                    if let Err(e) = &join_handle_data.mqtt_client.publish(&join_handle_data.conf.topic, qos, join_handle_data.conf.retain, data).await {
                                        warn!("{:?}", e);
                                        err = true;
                                    }
                                }
                                Err(e) => warn!("{:?}", e),
                            }

                        } else {
                            join_handle_data.message_retainer.push(mb);
                        }
                    }
                }
            }
        })
    }

    pub async fn update_conf(
        &mut self,
        _old_conf: SinkConf,
        new_conf: SinkConf,
    ) -> HaliaResult<()> {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = new_conf;
        Self::event_loop(join_handle_data);

        Ok(())
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_mqtt_client(&mut self, mqtt_client: Arc<AsyncClient>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.mqtt_client = mqtt_client;
        Self::event_loop(join_handle_data);
    }

    pub fn get_txs(&self, cnt: usize) -> Vec<UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];
        for _ in 0..cnt {
            txs.push(self.mb_tx.clone());
        }
        txs
    }
}

use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    sink_message_retain::{self, SinkMessageRetain},
};
use message::MessageBatch;
use rumqttc::{valid_topic, AsyncClient};
use tokio::{
    select,
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};
use tracing::warn;
use types::apps::mqtt_client_v311::SinkConf;

use super::transfer_qos;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

pub struct JoinHandleData {
    pub conf: SinkConf,
    pub message_retainer: Box<dyn SinkMessageRetain>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: mpsc::Receiver<MessageBatch>,
    pub mqtt_client: Arc<AsyncClient>,
    pub app_err_rx: broadcast::Receiver<bool>,
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
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let join_handle_data = JoinHandleData {
            conf,
            message_retainer,
            stop_signal_rx,
            mb_rx,
            mqtt_client,
            app_err_rx,
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
                        if !err {
                            if let Err(e) = &join_handle_data.mqtt_client.publish(&join_handle_data.conf.topic, qos, join_handle_data.conf.retain, mb.to_json()).await {
                                warn!("{:?}", e);
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
}

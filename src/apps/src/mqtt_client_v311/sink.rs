use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    sink_message_retain::{self, SinkMessageRetain},
};
use message::MessageBatch;
use rumqttc::{
    v5::mqttbytes::{self, v5::PublishProperties},
    AsyncClient,
};
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tracing::warn;
use types::apps::mqtt_client_v311::SinkConf;

pub struct Sink {
    conf: SinkConf,

    stop_signal_tx: mpsc::Sender<()>,

    pub mb_tx: mpsc::Sender<MessageBatch>,

    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
            Arc<AsyncClient>,
            broadcast::Receiver<bool>,
            Box<dyn SinkMessageRetain>,
        )>,
    >,
}

impl Sink {
    pub async fn new(
        conf: SinkConf,
        mqtt_client: Arc<AsyncClient>,
        app_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let (mb_tx, mb_rx) = mpsc::channel(16);
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);

        let message_retainer = sink_message_retain::new(&conf.message_retain);

        let mut sink = Self {
            conf,
            mb_tx,
            stop_signal_tx,
            join_handle: None,
        };
        sink.event_loop(
            halia_mqtt_client,
            app_err_rx,
            stop_signal_rx,
            mb_rx,
            message_retainer,
        );
        sink
    }

    fn event_loop(
        &mut self,
        mqtt_client: HaliaMqttClient,
        mut app_err_rx: broadcast::Receiver<bool>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        mut message_retainer: Box<dyn SinkMessageRetain>,
    ) {
        let topic = self.conf.topic.clone();
        let retain = self.conf.retain;
        let mut err = false;
        match halia_mqtt_client {
            HaliaMqttClient::V311(mqtt_client_v311) => {
                let qos = qos_to_v311(&self.conf.qos);
                let join_handle = tokio::spawn(async move {
                    loop {
                        select! {
                            _ = stop_signal_rx.recv() => {
                                return (stop_signal_rx, mb_rx, HaliaMqttClient::V311(mqtt_client_v311), app_err_rx, message_retainer);
                            }

                            chan_err = app_err_rx.recv() => {
                                match chan_err {
                                    Ok(chan_err) => err = chan_err,
                                    Err(e) => warn!("{:?}", e),
                                }
                            }

                            mb = mb_rx.recv() => {
                                match mb {
                                    Some(mb) => {
                                        if !err {
                                            if let Err(e) = mqtt_client_v311.publish(&topic, qos, retain, mb.to_json()).await {
                                                warn!("{:?}", e);
                                            }
                                        } else {
                                            message_retainer.push(mb);
                                        }

                                    }
                                    None => {}
                                }
                            }
                        }
                    }
                });
                self.join_handle = Some(join_handle);
            }
            HaliaMqttClient::V50(mqtt_client_v5) => {
                let qos = qos_to_v50(&self.conf.qos);
                let join_handle = tokio::spawn(async move {
                    loop {
                        select! {
                            _ = stop_signal_rx.recv() => {
                                return (stop_signal_rx, mb_rx, HaliaMqttClient::V50(mqtt_client_v5), app_err_rx, message_retainer);
                            }

                            mb = mb_rx.recv() => {
                                match mb {
                                    Some(mb) => {
                                        if !err {
                                            if let Err(e) = mqtt_client_v5.publish(&topic, qos, retain, mb.to_json()).await {
                                                warn!("{:?}", e);
                                            }
                                        } else {
                                            message_retainer.push(mb);
                                        }

                                    }
                                    None => {}
                                }
                            }
                        }
                    }
                });
                self.join_handle = Some(join_handle);
            }
        }
    }

    pub fn validate_conf(conf: &SinkConf) -> HaliaResult<()> {
        if !mqttbytes::valid_topic(&conf.topic) {
            return Err(HaliaError::Common("topic不合法！".to_owned()));
        }

        Ok(())
    }

    pub async fn update(&mut self, _old_conf: SinkConf, new_conf: SinkConf) -> HaliaResult<()> {
        let (stop_signal_rx, mb_rx, halia_mqtt_client, app_err_rx, message_retainer) =
            self.stop().await;

        self.conf = new_conf;

        self.event_loop(
            halia_mqtt_client,
            app_err_rx,
            stop_signal_rx,
            mb_rx,
            message_retainer,
        );

        Ok(())
    }

    pub async fn stop(
        &mut self,
    ) -> (
        mpsc::Receiver<()>,
        mpsc::Receiver<MessageBatch>,
        HaliaMqttClient,
        broadcast::Receiver<bool>,
        Box<dyn SinkMessageRetain>,
    ) {
        self.stop_signal_tx.send(()).await.unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn restart(&mut self, halia_mqtt_client: HaliaMqttClient) {
        let (stop_signal_rx, mb_rx, _, app_err_rx, message_retainer) = self.stop().await;
        self.event_loop(
            halia_mqtt_client,
            app_err_rx,
            stop_signal_rx,
            mb_rx,
            message_retainer,
        );
    }
}

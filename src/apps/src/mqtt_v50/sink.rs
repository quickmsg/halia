use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    sink_message_retain::{self, SinkMessageRetain},
};
use log::warn;
use message::MessageBatch;
use rumqttc::v5::{
    mqttbytes::{self, v5::PublishProperties},
    AsyncClient,
};
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use types::apps::mqtt_client_v50::SinkConf;

use super::transfer_qos;

pub struct Sink {
    stop_signal_tx: mpsc::Sender<()>,

    pub publish_properties: Option<PublishProperties>,

    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

pub struct JoinHandleData {
    mqtt_client: Arc<AsyncClient>,
    pub conf: SinkConf,
    pub message_retainer: Box<dyn SinkMessageRetain>,
    pub stop_signal_rx: mpsc::Receiver<()>,
    pub mb_rx: mpsc::Receiver<MessageBatch>,
    pub app_err_rx: broadcast::Receiver<bool>,
}

impl Sink {
    pub async fn new(
        conf: SinkConf,
        mqtt_client: Arc<AsyncClient>,
        app_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let publish_properties = get_publish_properties(&conf);
        let (mb_tx, mb_rx) = mpsc::channel(16);
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);

        let message_retainer = sink_message_retain::new(&conf.message_retain);
        let join_handle_data = JoinHandleData {
            mqtt_client,
            conf,
            message_retainer,
            stop_signal_rx,
            mb_rx,
            app_err_rx,
        };

        let join_handle = Self::event_loop(join_handle_data);

        Self {
            mb_tx,
            stop_signal_tx,
            publish_properties,
            join_handle: Some(join_handle),
        }
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        let mut err = false;
        let qos = transfer_qos(&join_handle_data.conf.qos);
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.recv() => {
                        return join_handle_data;
                    }

                    Some(mb) = join_handle_data.mb_rx.recv() => {
                        if !err {
                            if let Err(e) = join_handle_data.mqtt_client.publish(&join_handle_data.conf.topic, qos, join_handle_data.conf.retain, mb.to_json()).await {
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

    pub fn validate_conf(conf: &SinkConf) -> HaliaResult<()> {
        if !mqttbytes::valid_topic(&conf.topic) {
            return Err(HaliaError::Common("topic不合法！".to_owned()));
        }

        Ok(())
    }

    pub async fn update(&mut self, _old_conf: SinkConf, new_conf: SinkConf) -> HaliaResult<()> {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = new_conf;
        Self::event_loop(join_handle_data);

        Ok(())
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).await.unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_mqtt_client(&mut self, mqtt_client: Arc<AsyncClient>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.mqtt_client = mqtt_client;
        Self::event_loop(join_handle_data);
    }
}

fn get_publish_properties(conf: &SinkConf) -> Option<PublishProperties> {
    let mut some = false;
    let mut pp = PublishProperties {
        payload_format_indicator: None,
        message_expiry_interval: None,
        topic_alias: None,
        response_topic: None,
        correlation_data: None,
        user_properties: vec![],
        subscription_identifiers: vec![],
        content_type: None,
    };

    if let Some(pfi) = conf.payload_format_indicator {
        some = true;
        pp.payload_format_indicator = Some(pfi);
    }
    if let Some(mpi) = conf.message_expiry_interval {
        some = true;
        pp.message_expiry_interval = Some(mpi);
    }
    if let Some(ta) = conf.topic_alias {
        some = true;
        pp.topic_alias = Some(ta);
    }
    if let Some(cd) = &conf.correlation_data {
        some = true;
        todo!()
    }
    if let Some(up) = &conf.user_properties {
        some = true;
        pp.user_properties = up.clone();
    }
    if let Some(si) = &conf.subscription_identifiers {
        some = true;
        pp.subscription_identifiers = si.clone();
    }
    if let Some(ct) = &conf.content_type {
        some = true;
        pp.content_type = Some(ct.clone());
    }

    if some {
        Some(pp)
    } else {
        None
    }
}

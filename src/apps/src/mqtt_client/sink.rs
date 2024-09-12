use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    sink_message_retain::{self, SinkMessageRetain},
};
use message::MessageBatch;
use rumqttc::{
    v5::{
        self,
        mqttbytes::{self, v5::PublishProperties},
    },
    AsyncClient,
};
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tracing::warn;
use types::{apps::mqtt_client::SinkConf, BaseConf, SearchSourcesOrSinksInfoResp};

use super::{qos_to_v311, qos_to_v50, HaliaMqttClient};

pub struct Sink {
    conf: SinkConf,

    stop_signal_tx: mpsc::Sender<()>,

    pub publish_properties: Option<PublishProperties>,

    pub mb_tx: mpsc::Sender<MessageBatch>,

    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
            HaliaMqttClient,
            broadcast::Receiver<bool>,
            Box<dyn SinkMessageRetain>,
        )>,
    >,
}

impl Sink {
    pub async fn new(
        conf: SinkConf,
        halia_mqtt_client: HaliaMqttClient,
        app_err_rx: broadcast::Receiver<bool>,
    ) -> Self {
        let publish_properties = get_publish_properties(&conf);
        let (mb_tx, mb_rx) = mpsc::channel(16);
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);

        let mut sink = Self {
            conf,
            mb_tx,
            stop_signal_tx,
            join_handle: None,
            publish_properties,
        };
        sink.event_loop(halia_mqtt_client, app_err_rx, stop_signal_rx, mb_rx);
        sink
    }

    fn event_loop(
        &mut self,
        halia_mqtt_client: HaliaMqttClient,
        app_err_rx: broadcast::Receiver<bool>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) {
        let mut message_retainer = sink_message_retain::new(&self.conf.message_retain);
        let topic = self.conf.topic.clone();
        let retain = self.conf.retain;
        let mut app_err = false;
        match halia_mqtt_client {
            HaliaMqttClient::V311(mqtt_client_v311) => {
                let qos = qos_to_v311(&self.conf.qos);
                let join_handle = tokio::spawn(async move {
                    loop {
                        select! {
                            _ = stop_signal_rx.recv() => {
                                return (stop_signal_rx, mb_rx, HaliaMqttClient::V311(mqtt_client_v311), app_err_rx, message_retainer);
                            }

                            mb = mb_rx.recv() => {
                                match mb {
                                    Some(mb) => {
                                        if !app_err {
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
                                        if !app_err {
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

    fn validate_conf(conf: &SinkConf) -> HaliaResult<()> {
        if !mqttbytes::valid_topic(&conf.topic) {
            return Err(HaliaError::Common("topic不合法！".to_owned()));
        }

        Ok(())
    }

    // pub fn check_duplicate(&self, base_conf: &BaseConf, ext_conf: &SinkConf) -> HaliaResult<()> {
    //     if self.base_conf.name == base_conf.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     if self.ext_conf.topic == ext_conf.topic {
    //         return Err(HaliaError::Common("主题重复！".to_owned()));
    //     }

    //     Ok(())
    // }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        todo!()
    }

    pub async fn update(&mut self, base_conf: BaseConf, ext_conf: SinkConf) -> HaliaResult<()> {
        // Self::validate_conf(&ext_conf)?;

        // let mut restart = false;
        // if self.ext_conf != ext_conf {
        //     restart = true;
        // }
        // self.base_conf = base_conf;
        // self.ext_conf = ext_conf;

        // if restart && self.stop_signal_tx.is_some() {
        //     self.stop_signal_tx
        //         .as_ref()
        //         .unwrap()
        //         .send(())
        //         .await
        //         .unwrap();

        //     let (stop_signal_rx, mb_rx, v3_client, v5_client, app_err_rx, message_retainer) =
        //         self.join_handle.take().unwrap().await.unwrap();

        //     match (v3_client, v5_client) {
        //         (None, Some(v50_client)) => self.event_loop_v50(
        //             v50_client,
        //             stop_signal_rx,
        //             mb_rx,
        //             app_err_rx,
        //             message_retainer,
        //         ),
        //         (Some(v311_client), None) => self.event_loop_v311(
        //             v311_client,
        //             stop_signal_rx,
        //             mb_rx,
        //             app_err_rx,
        //             message_retainer,
        //         ),
        //         _ => unreachable!(),
        //     }
        // }

        Ok(())
    }

    pub async fn restart(&mut self, halia_mqtt_client: HaliaMqttClient) {}

    pub async fn restart_v311(&mut self, client: Arc<AsyncClient>) {
        self.stop_signal_tx.send(()).await.unwrap();
        let (stop_signal_rx, mb_rx, _, app_err_rx, message_retainer) =
            self.join_handle.take().unwrap().await.unwrap();

        // self.event_loop_v311(client, stop_signal_rx, mb_rx, app_err_rx, message_retainer);
    }

    pub async fn restart_v50(&mut self, client: Arc<v5::AsyncClient>) {
        self.stop_signal_tx.send(()).await.unwrap();

        let (stop_signal_rx, mb_rx, _, app_err_rx, message_retainer) =
            self.join_handle.take().unwrap().await.unwrap();

        let publish_properties = self.publish_properties.clone();

        // self.event_loop_v50(client, stop_signal_rx, mb_rx, app_err_rx, message_retainer);
        todo!()
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx.send(()).await.unwrap();
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

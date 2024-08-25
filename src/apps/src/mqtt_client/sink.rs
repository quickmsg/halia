use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
};
use message::MessageBatch;
use rumqttc::{
    v5::{
        self,
        mqttbytes::{self, v5::PublishProperties},
    },
    AsyncClient, QoS,
};
use tokio::{select, sync::mpsc, task::JoinHandle};
use types::{
    apps::mqtt_client::SinkConf, BaseConf, CreateUpdateSourceOrSinkReq,
    SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: SinkConf,

    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    pub publish_properties: Option<PublishProperties>,

    pub mb_tx: Option<mpsc::Sender<MessageBatch>>,

    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Receiver<MessageBatch>,
            Option<Arc<AsyncClient>>,
            Option<Arc<v5::AsyncClient>>,
        )>,
    >,
}

impl Sink {
    pub async fn new(sink_id: Uuid, base_conf: BaseConf, ext_conf: SinkConf) -> HaliaResult<Self> {
        Self::validate_conf(&ext_conf)?;

        let publish_properties = get_publish_properties(&ext_conf);

        Ok(Sink {
            id: sink_id,
            base_conf,
            ext_conf,
            mb_tx: None,
            stop_signal_tx: None,
            join_handle: None,
            publish_properties,
        })
    }

    fn validate_conf(conf: &SinkConf) -> HaliaResult<()> {
        if !mqttbytes::valid_topic(&conf.topic) {
            return Err(HaliaError::Common("topic不合法！".to_owned()));
        }

        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, ext_conf: &SinkConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        if self.ext_conf.topic == ext_conf.topic {
            return Err(HaliaError::Common("主题重复！".to_owned()));
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self, None)
    }

    pub async fn update(&mut self, base_conf: BaseConf, ext_conf: SinkConf) -> HaliaResult<()> {
        Self::validate_conf(&ext_conf)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        if restart && self.stop_signal_tx.is_some() {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (stop_signal_rx, mb_rx, v3_client, v5_client) =
                self.join_handle.take().unwrap().await.unwrap();

            match (v3_client, v5_client) {
                (None, Some(v50_client)) => self.event_loop_v50(v50_client, stop_signal_rx, mb_rx),
                (Some(v311_client), None) => {
                    self.event_loop_v311(v311_client, stop_signal_rx, mb_rx)
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    pub fn start_v311(&mut self, client: Arc<AsyncClient>) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(mb_tx);

        self.event_loop_v311(client, stop_signal_rx, mb_rx);
    }

    pub async fn restart_v311(&mut self, client: Arc<AsyncClient>) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        let (stop_signal_rx, mb_rx, _, _) = self.join_handle.take().unwrap().await.unwrap();

        self.event_loop_v311(client, stop_signal_rx, mb_rx);
    }

    fn event_loop_v311(
        &mut self,
        client: Arc<AsyncClient>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) {
        let topic = self.ext_conf.topic.clone();
        let qos = match self.ext_conf.qos {
            types::apps::mqtt_client::Qos::AtMostOnce => QoS::AtMostOnce,
            types::apps::mqtt_client::Qos::AtLeastOnce => QoS::AtLeastOnce,
            types::apps::mqtt_client::Qos::ExactlyOnce => QoS::AtLeastOnce,
        };
        let retain = self.ext_conf.retain;

        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, mb_rx, Some(client), None);
                    }

                    mb = mb_rx.recv() => {
                        match mb {
                            Some(mb) => {
                                let _ = client.publish(&topic, qos, retain, mb.to_json()).await;
                            }
                            None => {}
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    pub fn start_v50(&mut self, client: Arc<v5::AsyncClient>) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (tx, mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(tx);

        self.event_loop_v50(client, stop_signal_rx, mb_rx);
    }

    pub async fn restart_v50(&mut self, client: Arc<v5::AsyncClient>) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();

        let (stop_signal_rx, mb_rx, _, _) = self.join_handle.take().unwrap().await.unwrap();

        let publish_properties = self.publish_properties.clone();

        self.event_loop_v50(client, stop_signal_rx, mb_rx);
    }

    fn event_loop_v50(
        &mut self,
        client: Arc<v5::AsyncClient>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) {
        let topic = self.ext_conf.topic.clone();
        let qos = match self.ext_conf.qos {
            types::apps::mqtt_client::Qos::AtMostOnce => v5::mqttbytes::QoS::AtMostOnce,
            types::apps::mqtt_client::Qos::AtLeastOnce => v5::mqttbytes::QoS::AtLeastOnce,
            types::apps::mqtt_client::Qos::ExactlyOnce => v5::mqttbytes::QoS::ExactlyOnce,
        };
        let retain = self.ext_conf.retain;

        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, mb_rx, None, Some(client));
                    }

                    mb = mb_rx.recv() => {
                        match mb {
                            Some(mb) => {
                                let _ = client.publish(&topic, qos, retain, mb.to_json()).await;
                            }
                            None => {}
                        }
                    }
                }
            }
        });

        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.mb_tx = None;
        self.stop_signal_tx = None;
        self.join_handle = None;
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

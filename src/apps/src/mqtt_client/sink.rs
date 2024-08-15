use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    get_id, persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use rumqttc::{
    mqttbytes,
    v5::{self, mqttbytes::v5::PublishProperties},
    AsyncClient, QoS,
};
use tokio::{select, sync::mpsc, task::JoinHandle};
use types::apps::mqtt_client::{CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    pub publish_properties: Option<PublishProperties>,

    pub ref_info: RefInfo,
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
    pub async fn new(
        app_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<Self> {
        Sink::check_conf(&req)?;

        let (sink_id, new) = get_id(sink_id);
        if new {
            persistence::apps::mqtt_client::create_sink(
                app_id,
                &sink_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        let publish_properties = get_publish_properties(&req);

        Ok(Sink {
            id: sink_id,
            conf: req,
            mb_tx: None,
            ref_info: RefInfo::new(),
            stop_signal_tx: None,
            join_handle: None,
            publish_properties,
        })
    }

    fn check_conf(req: &CreateUpdateSinkReq) -> HaliaResult<()> {
        if !mqttbytes::valid_topic(&req.ext.topic) {
            return Err(HaliaError::Common("topic不合法！".to_owned()));
        }

        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateSinkReq) -> HaliaResult<()> {
        if self.conf.base.name == req.base.name {
            return Err(HaliaError::NameExists);
        }

        if self.conf.ext.topic == req.ext.topic {
            return Err(HaliaError::Common("主题重复！".to_owned()));
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn update(&mut self, app_id: &Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        Sink::check_conf(&req)?;

        persistence::apps::mqtt_client::update_sink(
            app_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

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
        let topic = self.conf.ext.topic.clone();
        let qos = match self.conf.ext.qos {
            types::apps::mqtt_client::Qos::AtMostOnce => QoS::AtMostOnce,
            types::apps::mqtt_client::Qos::AtLeastOnce => QoS::AtLeastOnce,
            types::apps::mqtt_client::Qos::ExactlyOnce => QoS::AtLeastOnce,
        };
        let retain = self.conf.ext.retain;

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
        let topic = self.conf.ext.topic.clone();
        let qos = match self.conf.ext.qos {
            types::apps::mqtt_client::Qos::AtMostOnce => v5::mqttbytes::QoS::AtMostOnce,
            types::apps::mqtt_client::Qos::AtLeastOnce => v5::mqttbytes::QoS::AtLeastOnce,
            types::apps::mqtt_client::Qos::ExactlyOnce => v5::mqttbytes::QoS::ExactlyOnce,
        };
        let retain = self.conf.ext.retain;

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

    pub async fn delete(&mut self, app_id: &Uuid) -> HaliaResult<()> {
        if self.ref_info.can_delete() {
            return Err(HaliaError::DeleteRefing);
        }
        persistence::apps::mqtt_client::delete_sink(app_id, &self.id).await?;
        Ok(())
    }

    pub fn get_mb_tx(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.mb_tx.as_ref().unwrap().clone()
    }

    pub fn del_mb_tx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id);
    }
}

fn get_publish_properties(req: &CreateUpdateSinkReq) -> Option<PublishProperties> {
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

    if let Some(pfi) = req.ext.payload_format_indicator {
        some = true;
        pp.payload_format_indicator = Some(pfi);
    }
    if let Some(mpi) = req.ext.message_expiry_interval {
        some = true;
        pp.message_expiry_interval = Some(mpi);
    }
    if let Some(ta) = req.ext.topic_alias {
        some = true;
        pp.topic_alias = Some(ta);
    }
    if let Some(cd) = &req.ext.correlation_data {
        some = true;
        todo!()
    }
    if let Some(up) = &req.ext.user_properties {
        some = true;
        pp.user_properties = up.clone();
    }
    if let Some(si) = &req.ext.subscription_identifiers {
        some = true;
        pp.subscription_identifiers = si.clone();
    }
    if let Some(ct) = &req.ext.content_type {
        some = true;
        pp.content_type = Some(ct.clone());
    }

    if some {
        Some(pp)
    } else {
        None
    }
}

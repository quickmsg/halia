use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
    ref_info::RefInfo,
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
use tracing::{debug, trace};
use types::apps::mqtt_client::{CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,

    pub publish_properties: Option<PublishProperties>,

    ref_info: RefInfo,
    pub mb_tx: Option<mpsc::Sender<MessageBatch>>,

    join_handle: Option<JoinHandle<(mpsc::Receiver<()>, mpsc::Receiver<MessageBatch>)>>,
}

impl Sink {
    pub async fn new(
        app_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<Self> {
        let (sink_id, new) = match sink_id {
            Some(sink_id) => (sink_id, false),
            None => (Uuid::new_v4(), true),
        };

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

    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            active_ref_rule_cnt: self.ref_info.active_ref_cnt(),
            ref_rule_cnt: self.ref_info.ref_cnt(),
        }
    }

    pub async fn update(&mut self, app_id: &Uuid, req: CreateUpdateSinkReq) -> HaliaResult<bool> {
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

        Ok(restart)
    }

    pub fn start_v311(&mut self, client: Arc<AsyncClient>) {
        trace!("sink start");
        let topic = self.conf.ext.topic.clone();
        let qos = match self.conf.ext.qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => unreachable!(),
        };
        let retain = self.conf.ext.retain;

        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mut mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(mb_tx);

        let handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        debug!("stop");
                        return (stop_signal_rx, mb_rx);
                    }

                    mb = mb_rx.recv() => {
                        match mb {
                            Some(mb) => {
                                trace!("sink get message");
                                let _ = client.publish(&topic, qos, retain, mb.to_json()).await;
                            }
                            None => {}
                        }
                    }
                }
            }
        });

        self.join_handle = Some(handle);
    }

    pub fn start_v50(&mut self, client: Arc<v5::AsyncClient>) {
        let topic = self.conf.ext.topic.clone();
        let qos = match self.conf.ext.qos {
            0 => mqttbytes::QoS::AtMostOnce,
            1 => mqttbytes::QoS::AtLeastOnce,
            2 => mqttbytes::QoS::ExactlyOnce,
            _ => unreachable!(),
        };
        let retain = self.conf.ext.retain;

        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (tx, mut rx) = mpsc::channel(16);
        self.mb_tx = Some(tx);

        let handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, rx);
                    }

                    mb = rx.recv() => {
                        match mb {
                            Some(mb) => {
                                let _ = client.publish(&topic, qos, retain, mb.to_json()).await;
                            }
                            None => unreachable!(),
                        }
                    }
                }
            }
        });

        self.join_handle = Some(handle);
    }

    pub async fn restart_v311(&mut self, client: Arc<AsyncClient>) {
        let topic = self.conf.ext.topic.clone();
        let qos = self.conf.ext.qos;
        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => unreachable!(),
        };
        let retain = self.conf.ext.retain;

        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();

        let (mut stop_signal_rx, mut rx) = self.join_handle.take().unwrap().await.unwrap();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return
                    }

                    mb = rx.recv() => {
                        match mb {
                            Some(mb) => {
                                let _ = client.publish(&topic, qos, retain, mb.to_json()).await;
                            }
                            None => unreachable!(),
                        }
                    }
                }
            }
        });
    }

    pub async fn restart_v50(&mut self, client: Arc<v5::AsyncClient>) {
        let topic = self.conf.ext.topic.clone();
        let qos = self.conf.ext.qos;
        let retain = self.conf.ext.retain;

        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();

        let (mut stop_signal_rx, mut rx) = self.join_handle.take().unwrap().await.unwrap();

        let publish_properties = self.publish_properties.clone();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return
                    }

                    mb = rx.recv() => {
                        match mb {
                            Some(mb) => {
                                match &publish_properties {
                                    Some(pp) => {
                                        let _ = client.publish_with_properties(&topic, mqttbytes::qos(qos).unwrap(), retain, mb.to_json(), pp.clone()).await;
                                    }
                                    None => {
                                        let _ = client.publish(&topic, mqttbytes::qos(qos).unwrap(), retain, mb.to_json()).await;
                                    }
                                }
                            }
                            None => unreachable!(),
                        }
                    }
                }
            }
        });
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
    }

    pub async fn delete(&mut self, app_id: &Uuid) -> HaliaResult<()> {
        if self.ref_info.can_delete() {
            return Err(HaliaError::Common("该动作正在被引用中".to_owned()));
        }

        persistence::apps::mqtt_client::delete_sink(app_id, &self.id).await?;
        Ok(())
    }

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.add_ref(rule_id);
    }

    pub fn get_mb_tx(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.mb_tx.as_ref().unwrap().clone()
    }

    pub fn del_mb_tx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id);
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.del_ref(rule_id);
    }

    pub fn can_stop(&self) -> bool {
        self.ref_info.can_stop()
    }

    pub fn can_delete(&self) -> bool {
        self.ref_info.can_delete()
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

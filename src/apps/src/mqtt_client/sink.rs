use std::sync::Arc;

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use rumqttc::{
    v5::{self, mqttbytes},
    AsyncClient, QoS,
};
use tokio::{select, sync::mpsc, task::JoinHandle};
use types::apps::mqtt_client::{CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
    pub tx: Option<mpsc::Sender<MessageBatch>>,
    pub stop_signal_tx: Option<mpsc::Sender<()>>,
    pub ref_cnt: usize,

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

        Ok(Sink {
            id: sink_id,
            conf: req,
            tx: None,
            ref_cnt: 0,
            stop_signal_tx: None,
            join_handle: None,
        })
    }

    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
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
        if self.conf.topic != req.topic || self.conf.qos != req.qos {
            restart = true;
        }

        self.conf = req;

        Ok(restart)
    }

    pub fn start_v311(&mut self, client: Arc<AsyncClient>) {
        let topic = self.conf.topic.clone();
        let qos = self.conf.qos;
        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => unreachable!(),
        };

        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (tx, mut rx) = mpsc::channel(16);
        self.tx = Some(tx);

        let handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, rx);
                    }

                    mb = rx.recv() => {
                        match mb {
                            Some(mb) => {
                                let _ = client.publish(&topic, qos, false, mb.to_json()).await;
                            }
                            None => unreachable!(),
                        }
                    }
                }
            }
        });

        self.join_handle = Some(handle);
    }

    pub fn start_v50(&mut self, client: Arc<v5::AsyncClient>) {
        let topic = self.conf.topic.clone();
        let qos = self.conf.qos;

        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (tx, mut rx) = mpsc::channel(16);
        self.tx = Some(tx);

        let handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, rx);
                    }

                    mb = rx.recv() => {
                        match mb {
                            Some(mb) => {
                                let _ = client.publish(&topic, mqttbytes::qos(qos).unwrap(), false, mb.to_json()).await;
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
        let topic = self.conf.topic.clone();
        let qos = self.conf.qos;
        let qos = match qos {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => unreachable!(),
        };
        let retain = self.conf.retain;

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
        let topic = self.conf.topic.clone();
        let qos = self.conf.qos;
        let retain = self.conf.retain;

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
                                let _ = client.publish(&topic, mqttbytes::qos(qos).unwrap(), retain, mb.to_json()).await;
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
        self.tx = None;
        self.stop_signal_tx = None;
    }

    pub async fn delete(&mut self, app_id: &Uuid) -> HaliaResult<()> {
        if self.ref_cnt > 0 {
            // TODO
            return Err(HaliaError::NotFound);
        }

        persistence::apps::mqtt_client::delete_sink(app_id, &self.id).await?;
        Ok(())
    }

    pub async fn unpublish(&mut self) {
        self.ref_cnt -= 1;
        if self.ref_cnt == 0 {
            self.stop().await;
        }
    }
}

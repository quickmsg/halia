use std::{net::SocketAddr, sync::Arc, time::Duration};

use common::{error::HaliaResult, persistence, ref_info::RefInfo};
use message::MessageBatch;
use protocol::coap::{
    client::UdpCoAPClient,
    request::{CoapRequest, Method, RequestBuilder},
};
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};
use tracing::debug;
use types::devices::coap::{CreateUpdateAPIReq, SearchAPIsItemResp};
use uuid::Uuid;

pub struct API {
    pub id: Uuid,
    conf: CreateUpdateAPIReq,
    request: CoapRequest<SocketAddr>,

    on: bool,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<JoinHandle<(mpsc::Receiver<()>, Arc<UdpCoAPClient>)>>,

    ref_info: RefInfo,
}

impl API {
    pub async fn new(
        device_id: &Uuid,
        api_id: Option<Uuid>,
        req: CreateUpdateAPIReq,
    ) -> HaliaResult<Self> {
        let (api_id, new) = match api_id {
            Some(api_id) => (api_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::coap::create_api(
                device_id,
                &api_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        let request = RequestBuilder::new(&req.ext.path, Method::Get)
            // .queries(todo!())
            .domain(req.ext.domain.clone())
            .build();

        Ok(Self {
            id: api_id,
            conf: req,
            request,
            on: false,
            stop_signal_tx: None,
            join_handle: None,
            ref_info: RefInfo::new(),
        })
    }

    pub fn search(&self) -> SearchAPIsItemResp {
        SearchAPIsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateAPIReq) -> HaliaResult<()> {
        persistence::devices::coap::update_api(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;
        if self.on && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();
            let (stop_signal_rx, client) = self.join_handle.take().unwrap().await.unwrap();
            self.event_loop(client, stop_signal_rx);
        }

        Ok(())
    }

    pub async fn delete(&self, device_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::coap::delete_api(device_id, &self.id).await?;
        if self.on {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();
        }
        Ok(())
    }

    pub async fn start(&mut self, client: Arc<UdpCoAPClient>) {
        if self.on {
            return;
        } else {
            self.on = true;
        }
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);
        self.event_loop(client, stop_signal_rx);
    }

    pub async fn stop(&mut self) {
        if !self.on {
            return;
        } else {
            self.on = false;
        }

        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;
    }

    fn event_loop(&mut self, client: Arc<UdpCoAPClient>, mut stop_signal_rx: mpsc::Receiver<()>) {
        let mut interval = time::interval(Duration::from_millis(self.conf.ext.interval));
        let request = self.request.clone();
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, client)
                    }

                    _ = interval.tick() => {
                        match client.send(request.clone()).await {
                            Ok(resp) => debug!("{:?}", resp),
                            Err(e) => debug!("{:?}", e),
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.add_ref(rule_id);
    }

    pub fn subscribe(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        // self.ref_info.subscribe(rule_id)
        todo!()
    }

    pub fn unsubscribe(&mut self, rule_id: &Uuid) {
        // self.ref_info.unsubscribe(rule_id);
        // todo!()
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.del_ref(rule_id);
    }
}

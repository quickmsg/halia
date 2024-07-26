use std::{net::SocketAddr, sync::Arc, time::Duration};

use common::{error::HaliaResult, persistence};
use protocol::coap::{
    client::UdpCoAPClient,
    request::{CoapRequest, Method, RequestBuilder},
};
use tokio::{select, sync::mpsc, task::JoinHandle, time};
use types::devices::coap::{CreateUpdateAPIReq, SearchAPIsItemResp};
use uuid::Uuid;

pub struct API {
    pub id: Uuid,
    conf: CreateUpdateAPIReq,
    pub request: CoapRequest<SocketAddr>,

    on: bool,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<JoinHandle<(mpsc::Receiver<()>, Arc<UdpCoAPClient>)>>,
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

        self.conf = req;

        Ok(())
    }

    pub async fn delete(&self, device_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::coap::delete_api(device_id, &self.id).await?;
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

    pub async fn stop(&mut self) {}

    fn event_loop(&mut self, client: Arc<UdpCoAPClient>, mut stop_signal_rx: mpsc::Receiver<()>) {
        let mut interval = time::interval(Duration::from_millis(self.conf.ext.interval));
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, client)
                    }

                    _ = interval.tick() => {

                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }
}

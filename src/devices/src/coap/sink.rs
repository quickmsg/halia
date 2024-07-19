use std::sync::Arc;

use common::{error::HaliaResult, persistence};
use message::MessageBatch;
use protocol::coap::{
    client::UdpCoAPClient,
    request::{Method, RequestBuilder},
};
use tokio::{select, sync::mpsc};
use types::devices::coap::{CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,

    ref_cnt: usize,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    publish_tx: Option<mpsc::Sender<MessageBatch>>,
}

impl Sink {
    pub async fn new(
        device_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<Self> {
        let (sink_id, new) = match sink_id {
            Some(sink_id) => (sink_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::coap::create_sink(
                device_id,
                &sink_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: sink_id,
            conf: req,
            ref_cnt: 0,
            stop_signal_tx: None,
            publish_tx: None,
        })
    }

    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        persistence::devices::coap::update_sink(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        self.conf = req;

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::coap::delete_sink(device_id, &self.id).await?;
        todo!()
    }

    pub fn start(&mut self, coap_client: Arc<UdpCoAPClient>) {
        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (publish_tx, mut publish_rx) = mpsc::channel(16);
        self.publish_tx = Some(publish_tx);

        let method = match &self.conf.method.as_str() {
            &"POST" => Method::Post,
            &"PUT" => Method::Put,
            &"DELETE" => Method::Delete,
            _ => unreachable!(),
        };

        let request = RequestBuilder::new(&self.conf.path, method)
            .domain(self.conf.domain.clone())
            .build();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return
                    }

                    mb = publish_rx.recv() => {
                        if let Some(mb) = mb {
                            match coap_client.send(request.clone()).await {
                                Ok(_) => todo!(),
                                Err(_) => todo!(),
                            }
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
        self.stop_signal_tx = None;
    }

    pub fn publish(&mut self) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        self.ref_cnt += 1;
        todo!()
    }

    pub async fn unpublish(&mut self) {
        self.ref_cnt -= 1;
    }
}

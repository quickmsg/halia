use common::{error::HaliaResult, persistence};
use futures::channel::mpsc;
use message::MessageBatch;
use types::devices::coap::{CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,

    ref_cnt: usize,
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

        todo!()
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

    pub fn publish(&mut self) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        self.ref_cnt += 1;
        todo!()
    }

    pub async fn unpublish(&mut self) {
        self.ref_cnt -= 1;
    }

    pub fn start(&mut self) {}

    pub fn stop(&mut self) {}
}

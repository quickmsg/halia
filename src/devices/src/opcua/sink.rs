use common::error::HaliaResult;
use message::MessageBatch;
use tokio::sync::mpsc;
use types::{CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    conf: CreateUpdateSourceOrSinkReq,
    pub mb_tx: Option<mpsc::Sender<MessageBatch>>,
}

impl Sink {
    pub async fn new(sink_id: Uuid, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<Self> {
        todo!()
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        SearchSourcesOrSinksInfoResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        todo!()
    }
}

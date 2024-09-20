use common::error::HaliaResult;
use message::MessageBatch;
use tokio::sync::mpsc;
use types::{CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp};

pub struct Sink {
    pub id: String,
    conf: CreateUpdateSourceOrSinkReq,
    pub mb_tx: Option<mpsc::Sender<MessageBatch>>,
}

impl Sink {
    pub async fn new(sink_id: String, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<Self> {
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
        device_id: &String,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        todo!()
    }
}

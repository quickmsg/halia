use std::sync::Arc;

use common::error::HaliaResult;
use message::MessageBatch;
use opcua::client::Session;
use tokio::sync::mpsc;
use types::{apps::http_client::SinkConf, CreateUpdateSourceOrSinkReq};

pub struct Sink {
    conf: CreateUpdateSourceOrSinkReq,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub async fn new(opcua_client: Arc<Session>, conf: SinkConf) -> HaliaResult<Self> {
        todo!()
    }

    pub async fn update_conf(&mut self, old_conf: SinkConf, new_conf: SinkConf) {
        todo!()
    }

    pub async fn stop(&mut self) {}
}

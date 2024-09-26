use std::sync::Arc;

use anyhow::Result;
use common::error::HaliaResult;
use message::MessageBatch;
use opcua::client::Session;
use tokio::sync::mpsc;
use types::apps::http_client::SinkConf;

pub struct Sink {
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub async fn new(opcua_client: Arc<Session>, conf: SinkConf) -> HaliaResult<Self> {
        todo!()
    }

    pub fn validate_conf(conf: SinkConf) -> Result<()> {
        todo!()
    }

    pub async fn update_conf(&mut self, old_conf: SinkConf, new_conf: SinkConf) {
        todo!()
    }

    pub async fn stop(&mut self) {}
}

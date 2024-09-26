use std::sync::Arc;

use anyhow::Result;
use message::MessageBatch;
use opcua::client::Session;
use tokio::sync::{mpsc, RwLock};
use types::apps::http_client::SinkConf;

pub struct Sink {
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub fn new(opcua_client: Arc<RwLock<Option<Arc<Session>>>>, conf: SinkConf) -> Self {
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

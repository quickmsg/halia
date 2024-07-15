use anyhow::Result;
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    pub conf: Conf,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: u8,
}

#[derive(Deserialize, Serialize)]
struct Conf {
    topic: String,
    qos: u8,
}

impl Source {
    pub fn new(source_id: Option<Uuid>, data: String) -> Result<Self> {
        todo!()
    }

    pub fn search(&self) {}

    pub fn update(&mut self, data: String) {}

    pub fn delete(&mut self) {}
}

use anyhow::Result;
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: Conf,
    pub tx: Option<mpsc::Receiver<MessageBatch>>,
    pub ref_cnt: u8,
}

#[derive(Deserialize, Serialize)]
struct Conf {
    topic: String,
    qos: u8,
}

impl Sink {
    pub fn new(source_id: Option<Uuid>, data: String) -> Result<Self> {
        todo!()
    }

    pub fn start(&self) {}

    pub fn stop() {}

    pub fn search(&self) {}

    pub fn update(&mut self, data: String) {}

    pub fn delete(&mut self) {}
}

use anyhow::Result;
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast::Sender;
use uuid::Uuid;

use crate::Source;

pub struct Device {
    conf: Conf,
    tx: Option<Sender<MessageBatch>>,
}

#[derive(Deserialize, Serialize)]
struct Conf {
    device_id: Uuid,
    group_id: Uuid,
}

impl Device {
    pub fn new(conf: Value) -> Result<Box<dyn Source>> {
        let conf: Conf = serde_json::from_value(conf.clone())?;
        Ok(Box::new(Device { conf, tx: None }))
    }
}

impl Source for Device {
    fn subscribe(&mut self) -> Result<tokio::sync::broadcast::Receiver<MessageBatch>> {
        todo!()
    }
}
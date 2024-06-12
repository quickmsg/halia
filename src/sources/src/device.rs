use anyhow::Result;
use async_trait::async_trait;
use common::error::HaliaResult;
use device::GLOBAL_DEVICE_MANAGER;
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast::{self, Sender};
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

#[async_trait]
impl Source for Device {
    async fn subscribe(&mut self) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        GLOBAL_DEVICE_MANAGER
            .subscribe(self.conf.device_id, self.conf.group_id)
            .await
    }
}

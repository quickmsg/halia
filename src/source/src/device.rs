use anyhow::Result;
use async_trait::async_trait;
use common::error::HaliaResult;
use device::GLOBAL_DEVICE_MANAGER;
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

use crate::Source;

static TYPE: &str = "device";

pub struct Device {
    id: Uuid,
    name: String,
    conf: Conf,
}

#[derive(Deserialize, Serialize, Clone)]
struct Conf {
    device_id: Uuid,
    group_id: Uuid,
}

impl Device {
    pub fn new(id: Uuid, req: &CreateSourceReq) -> HaliaResult<Box<dyn Source>> {
        let conf: Conf = serde_json::from_value(req.conf.clone())?;
        Ok(Box::new(Device {
            id,
            conf,
            name: req.name.clone(),
        }))
    }
}

#[async_trait]
impl Source for Device {
    async fn subscribe(&mut self) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        GLOBAL_DEVICE_MANAGER
            .subscribe(self.conf.device_id, self.conf.group_id)
            .await
    }

    fn get_type(&self) -> &'static str {
        return &TYPE;
    }

    fn get_info(&self) -> Result<ListSourceResp> {
        Ok(ListSourceResp {
            id: self.id,
            name: self.name.clone(),
            r#type: "device".to_string(),
        })
    }

    fn get_detail(&self) -> HaliaResult<SourceDetailResp> {
        Ok(SourceDetailResp {
            id: self.id.clone(),
            r#type: "device",
            name: self.name.clone(),
            conf: serde_json::json!(self.conf),
        })
    }

    fn stop(&self) {}

    fn update(&mut self, conf: serde_json::Value) -> HaliaResult<()> {
        todo!()
    }
}

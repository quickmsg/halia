#![feature(lazy_cell)]
use anyhow::{bail, Result};
use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use device::Device;
use message::MessageBatch;
use mqtt::Mqtt;
use std::collections::HashMap;
use std::sync::LazyLock;
use tokio::sync::broadcast::{self, Receiver};
use tokio::sync::RwLock;
use tracing::{debug, error};
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

pub mod device;
pub mod mqtt;

pub struct SourceManager {
    sources: RwLock<HashMap<Uuid, Box<dyn Source>>>,
}

pub static GLOBAL_SOURCE_MANAGER: LazyLock<SourceManager> = LazyLock::new(|| SourceManager {
    sources: RwLock::new(HashMap::new()),
});

impl SourceManager {
    pub async fn create(&self, id: Option<Uuid>, req: CreateSourceReq) -> HaliaResult<()> {
        let id = match id {
            Some(id) => id,
            None => Uuid::new_v4(),
        };
        match req.r#type.as_str() {
            "mqtt" => match Mqtt::new(id, req.conf.clone()) {
                Ok(mqtt) => {
                    debug!("insert source");
                    self.sources.write().await.insert(id, mqtt);
                    return Ok(());
                }
                Err(e) => {
                    error!("register souce:{} err:{}", req.name, e);
                    return Err(HaliaError::NotFound);
                }
            },
            "device" => match Device::new(id, req.conf.clone()) {
                Ok(device) => {
                    self.sources.write().await.insert(id, device);
                    return Ok(());
                }
                Err(e) => {
                    error!("register souce:{} err:{}", req.name, e);
                    return Err(HaliaError::NotFound);
                }
            },
            _ => return Err(HaliaError::ProtocolNotSupported),
        }
    }

    pub async fn read_source(&self, id: Uuid) -> HaliaResult<SourceDetailResp> {
        todo!()
    }

    pub async fn list(&self) -> HaliaResult<Vec<ListSourceResp>> {
        Ok(self
            .sources
            .read()
            .await
            .iter()
            .map(|(_, source)| source.get_info().unwrap())
            .collect())
    }

    pub async fn get_receiver(&self, id: Uuid) -> Result<Receiver<MessageBatch>> {
        debug!("subscribe source: {}", id);
        match self.sources.write().await.get_mut(&id) {
            Some(source) => match source.subscribe().await {
                Ok(x) => return Ok(x),
                Err(_) => todo!(),
            },
            None => {
                error!("don't have source:{}", id);
                bail!("not have source");
            }
        }
    }

    fn stop() {
        // todo!()
    }
}

#[async_trait]
pub trait Source: Send + Sync {
    async fn subscribe(&mut self) -> HaliaResult<broadcast::Receiver<MessageBatch>>;

    fn get_info(&self) -> Result<ListSourceResp>;

    fn stop(&self) {}
}

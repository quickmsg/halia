#![feature(lazy_cell)]
use anyhow::{bail, Result};
use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use common::persistence::{self, source};
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
        let (id, persistence) = match id {
            Some(id) => (id, false),
            None => (Uuid::new_v4(), true),
        };

        let source = match req.r#type.as_str() {
            "mqtt" => Mqtt::new(id, &req)?,
            "device" => Device::new(id, &req)?,
            _ => return Err(HaliaError::ProtocolNotSupported),
        };

        self.sources.write().await.insert(id, source);

        if persistence {
            if let Err(e) = source::insert(id, serde_json::to_string(&req).unwrap()).await {
                error!("isnert source err :{}", e);
            }
        }

        Ok(())
    }

    pub async fn read(&self, id: Uuid) -> HaliaResult<SourceDetailResp> {
        match self.sources.read().await.get(&id) {
            Some(source) => source.get_detail(),
            None => return Err(HaliaError::NotFound),
        }
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

    pub async fn update() {}

    pub async fn delete() {}

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

impl SourceManager {
    pub async fn recover(&self) -> HaliaResult<()> {
        let sources = match persistence::source::read().await {
            Ok(sources) => sources,
            Err(e) => {
                error!("read sources from file err:{}", e);
                return Err(e.into());
            }
        };

        for (id, status, data) in sources {
            let req = match serde_json::from_str::<CreateSourceReq>(&data) {
                Ok(req) => req,
                Err(e) => {
                    error!("parse json file err:{}", e);
                    return Err(e.into());
                }
            };
            GLOBAL_SOURCE_MANAGER.create(Some(id), req).await?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait Source: Send + Sync {
    async fn subscribe(&mut self) -> HaliaResult<broadcast::Receiver<MessageBatch>>;

    fn get_info(&self) -> Result<ListSourceResp>;

    fn get_detail(&self) -> HaliaResult<SourceDetailResp>;

    fn stop(&self) {}
}

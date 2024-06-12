use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use sources::Source;
use sources::{device::Device, mqtt::Mqtt};
use std::sync::LazyLock;
use tokio::sync::broadcast::Receiver;
use tokio::sync::RwLock;
use tracing::{debug, error};
use types::source::{CreateSourceReq, SourceDetailResp};
use uuid::Uuid;

pub struct SourceManager {
    pub sources: RwLock<Vec<(Uuid, Box<dyn Source>)>>,
}

pub static GLOBAL_SOURCE_MANAGER: LazyLock<SourceManager> = LazyLock::new(|| SourceManager {
    sources: RwLock::new(vec![]),
});

impl SourceManager {
    pub async fn create_source(&self, id: Option<Uuid>, req: CreateSourceReq) -> HaliaResult<()> {
        match req.r#type.as_str() {
            "mqtt" => match Mqtt::new(req.conf.clone()) {
                Ok(mqtt) => {
                    debug!("insert source");
                    self.sources.write().await.push((Uuid::new_v4(), mqtt));
                    return Ok(());
                }
                Err(e) => {
                    error!("register souce:{} err:{}", req.name, e);
                    return Err(HaliaError::NotFound);
                }
            },
            "device" => match Device::new(req.conf.clone()) {
                Ok(device) => {
                    self.sources.write().await.push((Uuid::new_v4(), device));
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

    pub async fn get_receiver(
        &self,
        source_id: Uuid,
        graph_name: String,
    ) -> Result<Receiver<MessageBatch>> {
        debug!("subscribe source: {}", source_id);
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|(id, _)| *id == source_id)
        {
            Some((_, source)) => match source.subscribe().await {
                Ok(x) => return Ok(x),
                Err(_) => todo!(),
            },
            None => {
                error!("don't have source:{}", source_id);
                bail!("not have source");
            }
        }
    }

    fn stop() {
        // todo!()
    }
}

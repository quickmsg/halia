use anyhow::{bail, Result};
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
use types::source::mqtt::{SearchTopicResp, TopicReq};
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

pub mod device;
// pub mod mqtt_bak;
mod mqtt;
// mod http_pull;

pub struct SourceManager {
    sources: RwLock<HashMap<Uuid, Source>>,
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
            "mqtt" => Source::Mqtt(Mqtt::new(id, &req)?),
            "device" => Source::Device(Device::new(id, &req)?),
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

    pub async fn subscribe(&self, id: Uuid) -> Result<Receiver<MessageBatch>> {
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
}

impl SourceManager {
    pub async fn create_topic(
        &self,
        mqtt_id: Uuid,
        topic_id: Option<Uuid>,
        req: TopicReq,
    ) -> HaliaResult<()> {
        match self.sources.write().await.get(&mqtt_id) {
            Some(source) => match source {
                Source::Mqtt(mqtt) => todo!(),
                _ => todo!(),
            },
            None => todo!(),
        }
    }

    pub async fn search_topic(
        &self,
        mqtt_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchTopicResp> {
        todo!()
    }

    pub async fn update_topic(
        &self,
        mqtt_id: Uuid,
        topic_id: Uuid,
        req: TopicReq,
    ) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn delete_topic(&self, mqtt_id: Uuid, topic_id: Uuid) -> HaliaResult<()> {
        Ok(())
    }
}

impl SourceManager {
    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::source::read().await {
            Ok(sources) => {
                for (id, data) in sources {
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
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => match persistence::source::init().await {
                    Ok(_) => Ok(()),
                    Err(e) => return Err(e.into()),
                },
                std::io::ErrorKind::PermissionDenied => todo!(),
                std::io::ErrorKind::ConnectionRefused => todo!(),
                std::io::ErrorKind::ConnectionReset => todo!(),
                std::io::ErrorKind::ConnectionAborted => todo!(),
                std::io::ErrorKind::NotConnected => todo!(),
                std::io::ErrorKind::AddrInUse => todo!(),
                std::io::ErrorKind::AddrNotAvailable => todo!(),
                std::io::ErrorKind::BrokenPipe => todo!(),
                std::io::ErrorKind::AlreadyExists => todo!(),
                std::io::ErrorKind::WouldBlock => todo!(),
                std::io::ErrorKind::InvalidInput => todo!(),
                std::io::ErrorKind::InvalidData => todo!(),
                std::io::ErrorKind::TimedOut => todo!(),
                std::io::ErrorKind::WriteZero => todo!(),
                std::io::ErrorKind::Interrupted => todo!(),
                std::io::ErrorKind::Unsupported => todo!(),
                std::io::ErrorKind::UnexpectedEof => todo!(),
                std::io::ErrorKind::OutOfMemory => todo!(),
                std::io::ErrorKind::Other => todo!(),
                _ => todo!(),
            },
        }
    }
}

enum Source {
    Mqtt(mqtt::Mqtt),
    Device(device::Device),
}

impl Source {
    fn get_detail(&self) -> HaliaResult<SourceDetailResp> {
        match self {
            Source::Mqtt(mqtt) => mqtt.get_detail(),
            Source::Device(_) => todo!(),
        }
    }

    fn get_info(&self) -> Result<ListSourceResp> {
        todo!()
    }

    async fn subscribe(&self) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        todo!()
    }
}

// #[async_trait]
// pub trait Source: Send + Sync {
//     async fn subscribe(&mut self) -> HaliaResult<broadcast::Receiver<MessageBatch>>;

//     fn get_type(&self) -> &'static str;

//     fn get_info(&self) -> Result<ListSourceResp>;

//     fn get_detail(&self) -> HaliaResult<SourceDetailResp>;

//     fn stop(&self);

//     fn update(&mut self, conf: Value) -> HaliaResult<()>;
// }

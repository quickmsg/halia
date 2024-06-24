use anyhow::{bail, Result};
use common::error::{HaliaError, HaliaResult};
use common::persistence::{self, source};
use device::Device;
use message::MessageBatch;
use mqttv31::Mqtt;
use std::collections::HashMap;
use std::sync::LazyLock;
use tokio::sync::broadcast::{self, Receiver};
use tokio::sync::RwLock;
use tracing::{debug, error};
use types::source::mqtt::{SearchTopicResp, TopicReq};
use types::source::{CreateSourceReq, ListSourceResp, SourceDetailResp};
use uuid::Uuid;

pub mod device;
mod http_pull;
mod mqtt_topic;
mod mqttv31;

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

    pub async fn subscribe(
        &self,
        source_id: Uuid,
        item_id: Option<Uuid>,
    ) -> Result<Receiver<MessageBatch>> {
        match self.sources.write().await.get_mut(&source_id) {
            Some(source) => match source.subscribe(item_id).await {
                Ok(x) => return Ok(x),
                Err(_) => todo!(),
            },
            None => {
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
        debug!("{mqtt_id:?} {topic_id:?}");
        match self.sources.write().await.get_mut(&mqtt_id) {
            Some(source) => match source {
                Source::Mqtt(mqtt) => mqtt.create_topic(topic_id, req).await,
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
        match self.sources.write().await.get_mut(&mqtt_id) {
            Some(source) => match source {
                Source::Mqtt(mqtt) => mqtt.search_topic(page, size).await,
                _ => todo!(),
            },
            None => todo!(),
        }
    }

    pub async fn update_topic(
        &self,
        mqtt_id: Uuid,
        topic_id: Uuid,
        req: TopicReq,
    ) -> HaliaResult<()> {
        match self.sources.write().await.get_mut(&mqtt_id) {
            Some(source) => match source {
                Source::Mqtt(mqtt) => mqtt.update_topic(topic_id, req).await,
                _ => todo!(),
            },
            None => todo!(),
        }
    }

    pub async fn delete_topic(&self, mqtt_id: Uuid, topic_id: Uuid) -> HaliaResult<()> {
        match self.sources.write().await.get_mut(&mqtt_id) {
            Some(source) => match source {
                Source::Mqtt(mqtt) => mqtt.delete_topic(topic_id).await,
                _ => todo!(),
            },
            None => todo!(),
        }
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
                    GLOBAL_SOURCE_MANAGER.create(Some(id), req.clone()).await?;
                    match req.r#type.as_str() {
                        "mqtt" => self.recover_mqtt_topics(id).await?,
                        _ => {}
                    }
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

    async fn recover_mqtt_topics(&self, source_id: Uuid) -> HaliaResult<()> {
        match persistence::source_item::read(source_id).await {
            Ok(items) => {
                for (id, data) in items {
                    match self
                        .create_topic(source_id, Some(id), serde_json::from_str(&data).unwrap())
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            error!("create topic err:{e:?}");
                        }
                    }
                }
                Ok(())
            }
            Err(_) => todo!(),
        }
    }
}

enum Source {
    Mqtt(mqttv31::Mqtt),
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
        match self {
            Source::Mqtt(mqtt) => mqtt.get_info(),
            Source::Device(_) => todo!(),
        }
    }

    async fn subscribe(
        &mut self,
        item_id: Option<Uuid>,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self {
            Source::Mqtt(mqtt) => mqtt.subscribe(item_id.unwrap()).await,
            Source::Device(_) => todo!(),
        }
    }
}

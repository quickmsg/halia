#![feature(duration_constants)]
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use std::{io, sync::LazyLock, vec};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error};
use types::apps::{
    CreateAppReq, SearchConnectorItemResp, SearchConnectorResp, SearchSinkResp, SearchSourceResp,
};
use uuid::Uuid;

mod mqtt_v311_client;
pub mod mqtt_v311_server;

pub struct AppManager {
    apps: RwLock<Vec<Box<dyn Connector>>>,
}

pub static GLOBAL_APP_MANAGER: LazyLock<AppManager> = LazyLock::new(|| AppManager {
    apps: RwLock::new(vec![]),
});

#[async_trait]
pub trait Connector: Sync + Send {
    fn get_id(&self) -> &Uuid;
    fn get_info(&self) -> SearchConnectorItemResp;

    async fn create_source(&self, req: &Bytes) -> HaliaResult<()>;
    async fn recover_source(&self, id: Uuid, req: String);
    async fn search_sources(&self, page: usize, size: usize) -> HaliaResult<SearchSourceResp>;
    async fn update_source(&self, source_id: Uuid, req: &Bytes) -> HaliaResult<()>;

    async fn subscribe(
        &mut self,
        source_id: Option<Uuid>,
    ) -> Result<broadcast::Receiver<MessageBatch>>;

    async fn create_sink(&self, req: &Bytes) -> HaliaResult<()>;
    async fn recover_sink(&self, id: Uuid, req: String);
    async fn search_sinks(&self, page: usize, size: usize) -> HaliaResult<SearchSinkResp>;
    async fn publish(&mut self, sink_id: Option<Uuid>) -> Result<mpsc::Sender<MessageBatch>>;
}

impl AppManager {
    async fn do_create_connector(&self, connector_id: Uuid, req: CreateAppReq) -> HaliaResult<()> {
        let connector = match req.r#type.as_str() {
            mqtt_v311_client::TYPE => mqtt_v311_client::new(connector_id, req),
            _ => return Err(HaliaError::ProtocolNotSupported),
        };

        match connector {
            Ok(connector) => {
                self.apps.write().await.push(connector);
                Ok(())
            }
            Err(e) => {
                debug!("{e}");
                Err(HaliaError::ConfErr)
            }
        }
    }
}

impl AppManager {
    pub async fn create_connector(&self, body: &Bytes) -> HaliaResult<()> {
        let req: CreateAppReq = serde_json::from_slice(body)?;
        let connector_id = Uuid::new_v4();
        self.do_create_connector(connector_id, req).await?;
        if let Err(e) = save_connector(&connector_id, body).await {
            error!("insert to file err :{e:?}");
        }
        Ok(())
    }

    pub async fn search_connectors(&self, page: usize, size: usize) -> SearchConnectorResp {
        let mut resp = vec![];
        let mut i = 0;
        let mut total = 0;
        for connector in self.apps.read().await.iter() {
            if i >= (page - 1) * size && i < page * size {
                resp.push(connector.get_info());
            }
            i += 1;
            total += 1;
        }
        SearchConnectorResp { total, data: resp }
    }

    pub async fn update_connector(&self, id: Uuid, body: &Bytes) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete_connector(&self, id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn create_source(&self, connector_id: &Uuid, req: &Bytes) -> HaliaResult<()> {
        match self
            .apps
            .read()
            .await
            .iter()
            .find(|c| c.get_id() == connector_id)
        {
            Some(c) => c.create_source(req).await,
            None => Err(HaliaError::ProtocolNotSupported),
        }
    }

    pub async fn search_source(
        &self,
        connector_id: &Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSourceResp> {
        match self
            .apps
            .read()
            .await
            .iter()
            .find(|c| c.get_id() == connector_id)
        {
            Some(c) => c.search_sources(page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_source(
        &self,
        connector_id: &Uuid,
        source_id: Uuid,
        req: &Bytes,
    ) -> HaliaResult<()> {
        match self
            .apps
            .read()
            .await
            .iter()
            .find(|c| c.get_id() == connector_id)
        {
            Some(c) => c.update_source(source_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn subscribe(
        &self,
        connector_id: &Uuid,
        item_id: Option<Uuid>,
    ) -> Result<broadcast::Receiver<MessageBatch>> {
        debug!("{},{:?}", connector_id, item_id);
        for connector in self.apps.write().await.iter_mut() {
            if connector.get_id() == connector_id {
                return connector.subscribe(item_id).await;
            }
        }

        bail!("not find")
    }

    pub async fn publish(
        &self,
        connector_id: &Uuid,
        item_id: Option<Uuid>,
    ) -> Result<mpsc::Sender<MessageBatch>> {
        for connector in self.apps.write().await.iter_mut() {
            if connector.get_id() == connector_id {
                return connector.publish(item_id).await;
            }
        }

        bail!("not find")
    }

    pub async fn create_sink(&self, connector_id: &Uuid, req: &Bytes) -> HaliaResult<()> {
        match self
            .apps
            .read()
            .await
            .iter()
            .find(|c| c.get_id() == connector_id)
        {
            Some(c) => c.create_sink(req).await,
            None => Err(HaliaError::ProtocolNotSupported),
        }
    }

    pub async fn search_sinks(
        &self,
        connector_id: &Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSinkResp> {
        match self
            .apps
            .read()
            .await
            .iter()
            .find(|c| c.get_id() == connector_id)
        {
            Some(c) => c.search_sinks(page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }
}

impl AppManager {
    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::connector::read_connectors().await {
            Ok(connectors) => {
                for (connector_id, data) in connectors {
                    let req: CreateAppReq = serde_json::from_str(&data)?;
                    self.do_create_connector(connector_id, req).await?;
                    match self
                        .apps
                        .read()
                        .await
                        .iter()
                        .find(|c| c.get_id() == &connector_id)
                    {
                        Some(c) => {
                            match persistence::connector::read_sources(&connector_id).await {
                                Ok(sources) => {
                                    for (source_id, data) in sources {
                                        c.recover_source(source_id, data).await;
                                    }
                                }
                                Err(e) => return Err(e.into()),
                            }

                            match persistence::connector::read_sinks(&connector_id).await {
                                Ok(sinks) => {
                                    for (sink_id, data) in sinks {
                                        c.recover_sink(sink_id, data).await;
                                    }
                                }
                                Err(e) => return Err(e.into()),
                            }
                        }
                        None => unreachable!(),
                    }
                }

                Ok(())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => match persistence::connector::init().await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("{e}");
                        return Err(e.into());
                    }
                },
                _ => {
                    error!("{e}");
                    return Err(e.into());
                }
            },
        }
    }
}

async fn save_connector(id: &Uuid, req: &Bytes) -> Result<(), io::Error> {
    persistence::connector::insert_connector(&id, req).await
}

async fn save_source(connector_id: &Uuid, source_id: &Uuid, req: &Bytes) -> Result<(), io::Error> {
    persistence::connector::insert_source(connector_id, source_id, req).await
}

async fn save_sink(connector_id: &Uuid, sink_id: &Uuid, req: &Bytes) -> Result<(), io::Error> {
    persistence::connector::insert_sink(connector_id, sink_id, req).await
}

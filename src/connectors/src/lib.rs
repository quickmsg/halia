use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use std::{sync::LazyLock, vec};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error};
use types::connector::{
    CreateConnectorReq, SearchConnectorItemResp, SearchConnectorResp, SearchSourceResp,
};
use uuid::Uuid;

mod mqtt_v311_client;
pub mod mqtt_v311_server;

pub struct ConnectorManager {
    connectors: RwLock<Vec<Box<dyn Connector>>>,
}

pub static GLOBAL_CONNECTOR_MANAGER: LazyLock<ConnectorManager> =
    LazyLock::new(|| ConnectorManager {
        connectors: RwLock::new(vec![]),
    });

#[async_trait]
pub trait Connector: Sync + Send {
    fn get_id(&self) -> &Uuid;

    fn get_info(&self) -> SearchConnectorItemResp;

    async fn create_source(&self, req: &Bytes) -> HaliaResult<()>;

    async fn search_source(&self, page: usize, size: usize) -> HaliaResult<SearchSourceResp>;

    async fn subscribe(
        &mut self,
        item_id: Option<Uuid>,
    ) -> Result<broadcast::Receiver<MessageBatch>>;
}

impl ConnectorManager {
    pub async fn api_create_connector(&self, body: &Bytes) -> HaliaResult<()> {
        let req: CreateConnectorReq = serde_json::from_slice(body)?;
        let connector_id = Uuid::new_v4();
        self.create_connector(connector_id, req).await?;
        unsafe {
            persistence::connector::insert(&connector_id, body).await?;
        }
        Ok(())
    }

    async fn persistence_create_connector(
        &self,
        connector_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        let req: CreateConnectorReq = serde_json::from_str(&data)?;
        self.create_connector(connector_id, req).await
    }

    async fn create_connector(
        &self,
        connector_id: Uuid,
        req: CreateConnectorReq,
    ) -> HaliaResult<()> {
        let connector = match req.r#type.as_str() {
            mqtt_v311_client::TYPE => mqtt_v311_client::new(connector_id, req),
            _ => return Err(HaliaError::ProtocolNotSupported),
        };

        match connector {
            Ok(connector) => {
                self.connectors.write().await.push(connector);
                Ok(())
            }
            Err(e) => {
                debug!("{e}");
                Err(HaliaError::ConfErr)
            }
        }
    }

    pub async fn search_connectors(&self, page: usize, size: usize) -> SearchConnectorResp {
        let mut resp = vec![];
        let mut i = 0;
        let mut total = 0;
        for connector in self.connectors.read().await.iter() {
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
            .connectors
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
            .connectors
            .read()
            .await
            .iter()
            .find(|c| c.get_id() == connector_id)
        {
            Some(c) => c.search_source(page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn subscribe(
        &self,
        connector_id: &Uuid,
        item_id: Option<Uuid>,
    ) -> Result<broadcast::Receiver<MessageBatch>> {
        for connector in self.connectors.write().await.iter_mut() {
            if connector.get_id() == connector_id {
                return connector.subscribe(item_id).await;
            }
        }

        bail!("not find")
    }
}

impl ConnectorManager {
    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::connector::read().await {
            Ok(connectors) => {
                for (id, data) in connectors {
                    if let Err(e) = self.persistence_create_connector(id, data).await {
                        error!("{}", e);
                        return Err(e.into());
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
                _ => todo!(),
            },
        }
    }
}

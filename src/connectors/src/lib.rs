use anyhow::{bail, Result};
use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use std::{sync::LazyLock, vec};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error};
use types::connector::{CreateConnectorReq, SearchConnectorResp};
use uuid::Uuid;

mod mqtt_v311_client;
pub mod mqtt_v311_server;

pub struct ConnectorManager {
    connectors: RwLock<Vec<Connector>>,
}

pub static GLOBAL_SOURCE_MANAGER: LazyLock<ConnectorManager> = LazyLock::new(|| ConnectorManager {
    connectors: RwLock::new(vec![]),
});

enum Connector {
    MqttV311(mqtt_v311_client::MqttV311),
}

impl Connector {
    fn get_id(&self) -> Uuid {
        match self {
            Connector::MqttV311(c) => c.id,
        }
    }

    async fn subscribe(&mut self, item_id: Uuid) -> Result<broadcast::Receiver<MessageBatch>> {
        match self {
            Connector::MqttV311(c) => c.subscribe(item_id).await,
        }
    }
}

impl ConnectorManager {
    pub async fn api_create_connector(&self, body: &Bytes) -> HaliaResult<()> {
        let req: CreateConnectorReq = serde_json::from_slice(body)?;
        let connector_id = Uuid::new_v4();
        self.create_connector(connector_id, &req).await?;
        todo!()
    }

    async fn persistence_create_connector(
        &self,
        connector_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        let req: CreateConnectorReq = serde_json::from_str(&data)?;
        self.create_connector(connector_id, &req).await
    }

    async fn create_connector(
        &self,
        connector_id: Uuid,
        req: &CreateConnectorReq,
    ) -> HaliaResult<()> {
        let connector = match req.r#type.as_str() {
            mqtt_v311_client::TYPE => mqtt_v311_client::new(connector_id, req),
            _ => todo!(),
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

    pub async fn search(&self, page: usize, size: usize) -> SearchConnectorResp {
        todo!()
    }

    pub async fn subscribe(
        &self,
        connector_id: Uuid,
        item_id: Uuid,
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
            Err(e) => return Err(e.into()),
        }
    }
}

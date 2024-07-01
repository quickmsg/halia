use std::{sync::LazyLock, vec};

use common::error::{HaliaError, HaliaResult};
use tokio::sync::RwLock;
use tracing::debug;
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

impl ConnectorManager {
    pub async fn create(&self, id: Option<Uuid>, req: &CreateConnectorReq) -> HaliaResult<()> {
        let result = match req.r#type.as_str() {
            mqtt_v311_client::TYPE => mqtt_v311_client::new(req),
            _ => todo!(),
        };
        match result {
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

    pub async fn subscribe(&self, connector_id: Uuid, item_id: Uuid) {}
}

use std::{sync::LazyLock, vec};

use common::error::HaliaResult;
use tokio::sync::RwLock;
use types::connector::CreateConnectorReq;
use uuid::Uuid;

pub mod mqtt_v311_client;
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
    pub async fn create(&self, id: Option<Uuid>, req: CreateConnectorReq) -> HaliaResult<()> {
        todo!()
    }
}

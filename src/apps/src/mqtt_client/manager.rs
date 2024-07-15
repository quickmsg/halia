use std::sync::LazyLock;

use common::error::HaliaResult;
use dashmap::DashMap;
use uuid::Uuid;

use super::MqttClient;

pub static GLOBAL_MQTT_CLIENT_MANAGER: LazyLock<Manager> = LazyLock::new(|| Manager {
    apps: DashMap::new(),
});

pub struct Manager {
    apps: DashMap<Uuid, MqttClient>,
}

impl Manager {
    pub async fn create(&self, app_id: Option<Uuid>, data: String) -> HaliaResult<()> {
        todo!()
    }

    pub async fn recover(&self, app_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn search(&self, app_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn update(&self, app_id: Uuid, data: String) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete(&self, app_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn create_source(
        &self,
        app_id: Uuid,
        source_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn search_sources(&self, app_id: Uuid, page: usize, size: usize) -> HaliaResult<()> {
        todo!()
    }

    pub async fn update_source(
        &self,
        app_id: Uuid,
        source_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete_source(&self, app_id: Uuid, source_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn subscribe(&self, app_id: &Uuid, source_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn unsubscribe(&self, app_id: &Uuid, source_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn create_sink(
        &self,
        app_id: Uuid,
        sink_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn search_sinks(&self, app_id: Uuid, page: usize, size: usize) -> HaliaResult<()> {
        todo!()
    }

    pub async fn update_sink(&self, app_id: Uuid, sink_id: Uuid, data: String) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete_sink(&self, app_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn publish(&self, app_id: &Uuid, sink_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn unpublish(&self, app_id: &Uuid, sink_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }
}

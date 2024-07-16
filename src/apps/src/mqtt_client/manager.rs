use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use types::apps::mqtt_client::{
    CreateUpdateMqttClientReq, CreateUpdateSourceReq, SearchSinksResp, SearchSourcesResp,
};
use uuid::Uuid;

use crate::{mqtt_client::TYPE, GLOBAL_APP_MANAGER};

use super::MqttClient;

pub static GLOBAL_MQTT_CLIENT_MANAGER: LazyLock<Manager> = LazyLock::new(|| Manager {
    apps: DashMap::new(),
});

pub struct Manager {
    apps: DashMap<Uuid, MqttClient>,
}

impl Manager {
    pub async fn create(
        &self,
        app_id: Option<Uuid>,
        req: CreateUpdateMqttClientReq,
    ) -> HaliaResult<()> {
        let app = MqttClient::new(app_id, req).await?;
        GLOBAL_APP_MANAGER.create(&TYPE, app.id.clone()).await;
        self.apps.insert(app.id.clone(), app);
        Ok(())
    }

    pub async fn recover(&self, app_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn search(&self, app_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn update(&self, app_id: Uuid, req: CreateUpdateMqttClientReq) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.update(req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete(&self, app_id: Uuid) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => {
                app.delete().await?;
                todo!()
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_source(
        &self,
        app_id: Uuid,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceReq,
    ) -> HaliaResult<()> {
        match self.apps.get(&app_id) {
            Some(app) => app.create_source(source_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_sources(
        &self,
        app_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSourcesResp> {
        match self.apps.get(&app_id) {
            Some(app) => app.search_sources(page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_source(
        &self,
        app_id: Uuid,
        source_id: Uuid,
        req: CreateUpdateSourceReq,
    ) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.update_source(source_id, req).await,
            None => Err(HaliaError::NotFound),
        }
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

    pub async fn search_sinks(
        &self,
        app_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSinksResp> {
        match self.apps.get(&app_id) {
            Some(app) => app.search_sinks(page, size).await,
            None => Err(HaliaError::NotFound),
        }
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

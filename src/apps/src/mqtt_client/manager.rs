use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};
use types::{
    apps::{
        mqtt_client::{
            CreateUpdateMqttClientReq, CreateUpdateSinkReq, CreateUpdateSourceReq, SearchSinksResp,
            SearchSourcesResp,
        },
        SearchAppsItemResp,
    },
    Pagination,
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

    pub async fn recover(&self) -> HaliaResult<()> {
        for mut app in self.apps.iter_mut() {
            app.recover().await?;
        }

        Ok(())
    }

    pub fn search(&self, app_id: &Uuid) -> HaliaResult<SearchAppsItemResp> {
        match self.apps.get(&app_id) {
            Some(app) => Ok(app.search()),
            None => Err(HaliaError::NotFound),
        }
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
            }
            None => return Err(HaliaError::NotFound),
        };

        self.apps.remove(&app_id);
        GLOBAL_APP_MANAGER.delete(&app_id).await;

        Ok(())
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
        pagination: Pagination,
    ) -> HaliaResult<SearchSourcesResp> {
        match self.apps.get(&app_id) {
            Some(app) => app.search_sources(pagination).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_source(
        &self,
        app_id: Uuid,
        source_id: Uuid,
        req: CreateUpdateSourceReq,
    ) -> HaliaResult<()> {
        match self.apps.get(&app_id) {
            Some(app) => app.update_source(source_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_source(&self, app_id: Uuid, source_id: Uuid) -> HaliaResult<()> {
        match self.apps.get(&app_id) {
            Some(app) => app.delete_source(source_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn get_source_mb_rx(
        &self,
        app_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.get_source_mb_rx(source_id, rule_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn del_source_mb_rx(
        &self,
        app_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.del_source_mb_rx(source_id, rule_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_sink(
        &self,
        app_id: Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.create_sink(sink_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_sinks(
        &self,
        app_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchSinksResp> {
        match self.apps.get(&app_id) {
            Some(app) => Ok(app.search_sinks(pagination).await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_sink(
        &self,
        app_id: Uuid,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.update_sink(sink_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_sink(&self, app_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.delete_sink(sink_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn publish(
        &self,
        app_id: &Uuid,
        sink_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.publish(sink_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn unpublish(&self, app_id: &Uuid, sink_id: &Uuid) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.unpublish(sink_id).await,
            None => Err(HaliaError::NotFound),
        }
    }
}

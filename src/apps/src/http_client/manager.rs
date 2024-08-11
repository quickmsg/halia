use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use types::{
    apps::{
        http_client::{CreateUpdateHttpClientReq, CreateUpdateSinkReq, SearchSinksResp},
        SearchAppsItemResp,
    },
    Pagination,
};
use uuid::Uuid;

use crate::GLOBAL_APP_MANAGER;

use super::{HttpClient, TYPE};

macro_rules! http_client_not_find_err {
    ($app_id:expr) => {
        Err(HaliaError::NotFound("http客户端".to_owned(), $app_id))
    };
}

pub static GLOBAL_HTTP_CLIENT_MANAGER: LazyLock<Manager> = LazyLock::new(|| Manager {
    apps: DashMap::new(),
});

pub struct Manager {
    apps: DashMap<Uuid, HttpClient>,
}

impl Manager {
    pub async fn create(
        &self,
        app_id: Option<Uuid>,
        req: CreateUpdateHttpClientReq,
    ) -> HaliaResult<()> {
        let app = HttpClient::new(app_id, req).await?;
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
            None => http_client_not_find_err!(app_id.clone()),
        }
    }

    pub async fn update(&self, app_id: Uuid, req: CreateUpdateHttpClientReq) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.update(req).await,
            None => http_client_not_find_err!(app_id),
        }
    }

    pub async fn start(&self, app_id: Uuid) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.start().await,
            None => http_client_not_find_err!(app_id),
        }
    }

    pub async fn stop(&self, app_id: Uuid) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.stop().await,
            None => http_client_not_find_err!(app_id),
        }
    }

    pub async fn delete(&self, app_id: Uuid) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => {
                app.delete().await?;
            }
            None => return http_client_not_find_err!(app_id),
        };

        self.apps.remove(&app_id);
        GLOBAL_APP_MANAGER.delete(&app_id).await;

        Ok(())
    }

    pub async fn create_sink(
        &self,
        app_id: Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.create_sink(sink_id, req).await,
            None => http_client_not_find_err!(app_id),
        }
    }

    pub async fn search_sinks(
        &self,
        app_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchSinksResp> {
        match self.apps.get(&app_id) {
            Some(app) => Ok(app.search_sinks(pagination).await),
            None => http_client_not_find_err!(app_id),
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
            None => http_client_not_find_err!(app_id),
        }
    }

    pub async fn delete_sink(&self, app_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        match self.apps.get_mut(&app_id) {
            Some(mut app) => app.delete_sink(sink_id).await,
            None => http_client_not_find_err!(app_id),
        }
    }
}

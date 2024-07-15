#![feature(duration_constants)]
use common::error::HaliaResult;
use std::{sync::LazyLock, vec};
use tokio::sync::RwLock;
use types::apps::SearchAppsResp;
use uuid::Uuid;

pub mod mqtt_client;

pub struct AppManager {
    apps: RwLock<Vec<(&'static str, Uuid)>>,
}

pub static GLOBAL_APP_MANAGER: LazyLock<AppManager> = LazyLock::new(|| AppManager {
    apps: RwLock::new(vec![]),
});

impl AppManager {
    pub async fn create(&self, r#type: &'static str, app_id: Uuid) {
        self.apps.write().await.push((r#type, app_id));
    }

    pub async fn search(&self, page: usize, size: usize) -> HaliaResult<SearchAppsResp> {
        todo!()
    }

    pub async fn delete(&self, app_id: &Uuid) {
        self.apps.write().await.retain(|(_, id)| id == app_id);
    }
}

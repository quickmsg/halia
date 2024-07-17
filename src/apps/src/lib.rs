#![feature(duration_constants)]
use common::{error::HaliaResult, persistence};
use mqtt_client_v311::manager::GLOBAL_MQTT_CLIENT_V311_MANAGER;
use mqtt_client_v50::manager::GLOBAL_MQTT_CLIENT_V50_MANAGER;
use std::{str::FromStr, sync::LazyLock, vec};
use tokio::sync::RwLock;
use tracing::debug;
use types::apps::SearchAppsResp;
use uuid::Uuid;

pub mod mqtt_client_v311;
pub mod mqtt_client_v50;

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
        let mut data = vec![];
        for (r#type, app_id) in self.apps.read().await.iter().rev() {
            match r#type {
                &mqtt_client_v311::TYPE => match GLOBAL_MQTT_CLIENT_V311_MANAGER.search(app_id) {
                    Ok(info) => {
                        data.push(info);
                    }
                    Err(e) => return Err(e),
                },
                &mqtt_client_v50::TYPE => match GLOBAL_MQTT_CLIENT_V50_MANAGER.search(app_id) {
                    Ok(info) => {
                        data.push(info);
                    }
                    Err(e) => return Err(e),
                },
                _ => {}
            }
        }

        Ok(SearchAppsResp {
            total: self.apps.read().await.len(),
            data,
        })
    }

    pub async fn delete(&self, app_id: &Uuid) {
        self.apps.write().await.retain(|(_, id)| id == app_id);
    }

    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::apps::read_apps().await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    debug!("{}", data);
                    let items: Vec<&str> = data.split(persistence::DELIMITER).collect();
                    debug!("{:?} {}", items, items.len());
                    assert!(items.len() == 3, "数据错误");
                    let app_id = Uuid::from_str(items[0]).unwrap();
                    match items[1] {
                        mqtt_client_v311::TYPE => {
                            GLOBAL_MQTT_CLIENT_V311_MANAGER
                                .create(Some(app_id), serde_json::from_str(items[2]).unwrap())
                                .await?;
                        }
                        _ => {}
                    }
                }

                GLOBAL_MQTT_CLIENT_V311_MANAGER.recover().await?;

                Ok(())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => match persistence::apps::init().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.into()),
                },
                _ => Err(e.into()),
            },
        }
    }
}

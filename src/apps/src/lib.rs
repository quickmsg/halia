#![feature(duration_constants)]
use common::{error::HaliaResult, persistence};
use http_client::manager::GLOBAL_HTTP_CLIENT_MANAGER;
use mqtt_client::manager::GLOBAL_MQTT_CLIENT_MANAGER;
use std::{str::FromStr, sync::LazyLock, vec};
use tokio::sync::RwLock;
use tracing::warn;
use types::{
    apps::{SearchAppsResp, Summary},
    Pagination,
};
use uuid::Uuid;

pub mod http_client;
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

    pub async fn get_summary(&self) -> Summary {
        let mut total = 0;
        let mut running_cnt = 0;
        let mut err_cnt = 0;
        let mut off_cnt = 0;
        for (typ, app_id) in self.apps.read().await.iter() {
            let resp = match typ {
                &mqtt_client::TYPE => GLOBAL_MQTT_CLIENT_MANAGER.search(app_id),
                &http_client::TYPE => GLOBAL_HTTP_CLIENT_MANAGER.search(app_id),
                _ => unreachable!(),
            };

            match resp {
                Ok(resp) => {
                    total += 1;
                    if resp.err.is_some() {
                        err_cnt += 1;
                    } else {
                        if resp.on {
                            running_cnt += 1;
                        } else {
                            off_cnt += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!("{}", e);
                }
            }
        }
        Summary {
            total,
            running_cnt,
            err_cnt,
            off_cnt,
        }
    }

    pub async fn search(&self, pagination: Pagination) -> HaliaResult<SearchAppsResp> {
        let mut data = vec![];
        for (r#type, app_id) in self
            .apps
            .read()
            .await
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            match r#type {
                &mqtt_client::TYPE => match GLOBAL_MQTT_CLIENT_MANAGER.search(app_id) {
                    Ok(info) => {
                        data.push(info);
                    }
                    Err(e) => return Err(e),
                },
                _ => {}
            }
            if data.len() == pagination.size {
                break;
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
                    let items: Vec<&str> = data.split(persistence::DELIMITER).collect();
                    assert_eq!(items.len(), 4);
                    let app_id = Uuid::from_str(items[0]).unwrap();
                    match items[1] {
                        mqtt_client::TYPE => {
                            GLOBAL_MQTT_CLIENT_MANAGER
                                .create(Some(app_id), serde_json::from_str(items[3]).unwrap())
                                .await?;
                            match items[2] {
                                "0" => {}
                                "1" => {
                                    GLOBAL_MQTT_CLIENT_MANAGER.start(app_id).await.unwrap();
                                }
                                _ => {
                                    panic!("缓存文件错误")
                                }
                            }
                        }
                        _ => {}
                    }
                }

                GLOBAL_MQTT_CLIENT_MANAGER.recover().await?;

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

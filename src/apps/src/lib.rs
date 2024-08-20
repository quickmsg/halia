#![feature(duration_constants)]
use common::{error::HaliaResult, persistence};
use message::MessageBatch;
use std::{str::FromStr, sync::LazyLock, vec};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::warn;
use types::{
    apps::{AppType, QueryParams, SearchAppsResp, Summary},
    Pagination,
};
use uuid::Uuid;

pub mod http_client;
pub mod mqtt_client;
pub mod mqtt_server;

pub struct AppManager {
    apps: RwLock<Vec<(AppType, Uuid)>>,
}

pub static GLOBAL_APP_MANAGER: LazyLock<AppManager> = LazyLock::new(|| AppManager {
    apps: RwLock::new(vec![]),
});

macro_rules! app_not_found_err {
    () => {
        Err(HaliaError::NotFound("应用".to_owned()))
    };
}

#[macro_export]
macro_rules! source_not_found_err {
    () => {
        Err(HaliaError::NotFound("源".to_owned()))
    };
}

#[macro_export]
macro_rules! sink_not_found_err {
    () => {
        Err(HaliaError::NotFound("动作".to_owned()))
    };
}

impl AppManager {
    pub async fn create(&self, typ: AppType, app_id: Uuid) {
        self.apps.write().await.push((typ, app_id));
    }

    pub async fn get_summary(&self) -> Summary {
        todo!()
        // let mut total = 0;
        // let mut running_cnt = 0;
        // let mut err_cnt = 0;
        // let mut off_cnt = 0;
        // for (typ, app_id) in self.apps.read().await.iter() {
        //     let resp = match typ {
        //         AppType::MqttClient => GLOBAL_MQTT_CLIENT_MANAGER.search(app_id).await,
        //         AppType::HttpClient => GLOBAL_HTTP_CLIENT_MANAGER.search(app_id),
        //     };

        //     match resp {
        //         Ok(resp) => {
        //             total += 1;
        //             if resp.err.is_some() {
        //                 err_cnt += 1;
        //             } else {
        //                 if resp.on {
        //                     running_cnt += 1;
        //                 } else {
        //                     off_cnt += 1;
        //                 }
        //             }
        //         }
        //         Err(e) => {
        //             warn!("{}", e);
        //         }
        //     }
        // }
        // Summary {
        //     total,
        //     running_cnt,
        //     err_cnt,
        //     off_cnt,
        // }
    }

    pub async fn search(
        &self,
        pagination: Pagination,
        query_params: QueryParams,
    ) -> SearchAppsResp {
        todo!()
        // let mut data = vec![];
        // let mut i = 0;
        // let mut total = 0;

        // for (typ, app_id) in self.apps.read().await.iter().rev() {
        //     if let Some(query_type) = &query_params.typ {
        //         if typ != query_type {
        //             continue;
        //         }
        //     }

        //     let resp = match typ {
        //         AppType::MqttClient => GLOBAL_MQTT_CLIENT_MANAGER.search(app_id).await,
        //         AppType::HttpClient => GLOBAL_HTTP_CLIENT_MANAGER.search(app_id),
        //     };

        //     match resp {
        //         Ok(resp) => {
        //             if let Some(query_name) = &query_params.name {
        //                 if !resp.conf.base.name.contains(query_name) {
        //                     continue;
        //                 }
        //             }

        //             if let Some(on) = &query_params.on {
        //                 if resp.on != *on {
        //                     continue;
        //                 }
        //             }

        //             if let Some(err) = &query_params.err {
        //                 if resp.err.is_some() != *err {
        //                     continue;
        //                 }
        //             }

        //             if i >= (pagination.page - 1) * pagination.size
        //                 && i < pagination.page * pagination.size
        //             {
        //                 data.push(resp);
        //             }

        //             total += 1;
        //             i += 1;
        //         }
        //         Err(e) => {
        //             warn!("{}", e);
        //         }
        //     }
        // }

        // SearchAppsResp { total, data }
    }

    pub async fn delete(&self, app_id: &Uuid) {
        self.apps.write().await.retain(|(_, id)| id != app_id);
    }

    pub async fn recover(&self) -> HaliaResult<()> {
        // match persistence::apps::read_apps().await {
        //     Ok(datas) => {
        //         for data in datas {
        //             if data.len() == 0 {
        //                 continue;
        //             }
        //             let items: Vec<&str> = data.split(persistence::DELIMITER).collect();
        //             assert_eq!(items.len(), 4);
        //             let app_id = Uuid::from_str(items[0]).unwrap();

        //             let typ = AppType::try_from(items[1]);
        //             match typ {
        //                 Ok(typ) => match typ {
        //                     AppType::MqttClient => {
        //                         GLOBAL_MQTT_CLIENT_MANAGER
        //                             .create(Some(app_id), serde_json::from_str(items[3]).unwrap())
        //                             .await?;
        //                         match items[2] {
        //                             "0" => {}
        //                             "1" => {
        //                                 GLOBAL_MQTT_CLIENT_MANAGER.start(app_id).await.unwrap();
        //                             }
        //                             _ => {
        //                                 panic!("缓存文件错误")
        //                             }
        //                         }
        //                     }
        //                     AppType::HttpClient => todo!(),
        //                 },
        //                 Err(e) => panic!("{}", e),
        //             }
        //         }

        //         GLOBAL_MQTT_CLIENT_MANAGER.recover().await?;

        //         Ok(())
        //     }
        //     Err(e) => match e.kind() {
        //         std::io::ErrorKind::NotFound => match persistence::apps::init().await {
        //             Ok(_) => Ok(()),
        //             Err(e) => Err(e.into()),
        //         },
        //         _ => Err(e.into()),
        //     },
        // }
        todo!()
    }
}

impl AppManager {
    pub async fn get_source_rx(
        &self,
        app_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        todo!()
    }

    pub async fn get_sink_tx(
        &self,
        app_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        todo!()
    }
}

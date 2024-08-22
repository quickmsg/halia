#![feature(duration_constants)]
use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use std::{str::FromStr, sync::LazyLock, vec};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::warn;
use types::{
    apps::{AppType, CreateUpdateAppReq, QueryParams, SearchAppsItemResp, SearchAppsResp, Summary},
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp,
};
use uuid::Uuid;

pub mod http_client;
pub mod mqtt_client;
pub mod mqtt_server;

#[async_trait]
pub trait App: Send + Sync {
    fn get_id(&self) -> &Uuid;
    async fn search(&self) -> SearchAppsItemResp;
    async fn update(&mut self, req: CreateUpdateAppReq) -> HaliaResult<()>;
    async fn start(&mut self) -> HaliaResult<()>;
    async fn stop(&mut self) -> HaliaResult<()>;
    async fn delete(&mut self) -> HaliaResult<()>;

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn search_sources(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp;
    async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()>;

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn search_sinks(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp;
    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()>;

    async fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>>;
    async fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;

    async fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>>;
    async fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
}

pub static GLOBAL_APP_MANAGER: LazyLock<AppManager> = LazyLock::new(|| AppManager {
    apps: RwLock::new(vec![]),
});

pub struct AppManager {
    apps: RwLock<Vec<Box<dyn App>>>,
}

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
    // pub async fn create(&self, typ: AppType, app_id: Uuid) {
    //     self.apps.write().await.push((typ, app_id));
    // }

    // pub async fn delete(&self, app_id: &Uuid) {
    //     self.apps.write().await.retain(|(_, id)| id != app_id);
    // }

    pub async fn recover(&self) -> HaliaResult<()> {
        // match persistence::read_apps().await {
        //     Ok(datas) => {
        //         for data in datas {
        //             let items: Vec<&str> = data.split(persistence::DELIMITER).collect();
        //             assert_eq!(items.len(), 3);
        //             let app_id = Uuid::from_str(items[0]).unwrap();
        //             // let req: CreateUpdat

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
        Ok(())
    }
}

impl AppManager {
    pub async fn get_summary(&self) -> Summary {
        let mut total = 0;
        let mut running_cnt = 0;
        let mut err_cnt = 0;
        let mut off_cnt = 0;
        for app in self.apps.read().await.iter().rev() {
            let app = app.search().await;
            total += 1;

            if app.err.is_some() {
                err_cnt += 1;
            } else {
                if app.on {
                    running_cnt += 1;
                } else {
                    off_cnt += 1;
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

    pub async fn create_app(
        &self,
        app_id: Uuid,
        req: CreateUpdateAppReq,
        recover: bool,
    ) -> HaliaResult<()> {
        let data = serde_json::to_string(&req)?;
        let device = match req.typ {
            AppType::MqttClient => mqtt_client::new(app_id, req.conf).await?,
            AppType::HttpClient => http_client::new(app_id, req.conf).await?,
        };

        if !recover {
            persistence::create_app(&app_id, &data).await?;
        }
        self.apps.write().await.push(device);
        Ok(())
    }

    pub async fn search_apps(&self, pagination: Pagination, query: QueryParams) -> SearchAppsResp {
        todo!()
    }

    pub async fn update_app(&self, app_id: Uuid, req: CreateUpdateAppReq) -> HaliaResult<()> {
        todo!()
    }

    pub async fn start_app(&self, app_id: Uuid) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => app.start().await,
            None => app_not_found_err!(),
        }
    }

    pub async fn stop_app(&self, app_id: Uuid) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => app.stop().await,
            None => app_not_found_err!(),
        }
    }

    pub async fn delete_app(&self, app_id: Uuid) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => app.delete().await?,
            None => return app_not_found_err!(),
        }

        self.apps
            .write()
            .await
            .retain(|app| *app.get_id() != app_id);

        Ok(())
    }
}

impl AppManager {
    pub async fn create_source(
        &self,
        app_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => {
                let data = serde_json::to_string(&req)?;
                let source_id = Uuid::new_v4();
                app.create_source(source_id, req).await;
                persistence::create_source(app.get_id(), &source_id, &data).await?;
                Ok(())
            }
            None => app_not_found_err!(),
        }
    }

    pub async fn search_sources(
        &self,
        app_id: Uuid,
        pagination: Pagination,
        query: QueryParams,
    ) -> HaliaResult<SearchSourcesOrSinksResp> {
        match self
            .apps
            .read()
            .await
            .iter()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => Ok(app.search_sources(pagination, query).await),
            None => app_not_found_err!(),
        }
    }

    pub async fn update_source(
        &self,
        app_id: Uuid,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => app.update_source(source_id, req).await,
            None => app_not_found_err!(),
        }
    }

    pub async fn delete_source(&self, app_id: Uuid, source_id: Uuid) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => app.delete_source(source_id).await,
            None => app_not_found_err!(),
        }
    }
}

impl AppManager {
    pub async fn create_sink(
        &self,
        app_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => {
                let data = serde_json::to_string(&req)?;
                let sink_id = Uuid::new_v4();
                app.create_sink(sink_id, req).await?;
                persistence::create_sink(app.get_id(), &sink_id, &data).await?;
                Ok(())
            }
            None => app_not_found_err!(),
        }
    }

    pub async fn search_sinks(
        &self,
        app_id: Uuid,
        pagination: Pagination,
        query: QueryParams,
    ) -> HaliaResult<SearchSourcesOrSinksResp> {
        match self
            .apps
            .read()
            .await
            .iter()
            .find(|app| *app.get_id() == app_id)
        {
            Some(device) => Ok(device.search_sinks(pagination, query).await),
            None => app_not_found_err!(),
        }
    }

    pub async fn update_sink(
        &self,
        app_id: Uuid,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => app.update_sink(sink_id, req).await,
            None => app_not_found_err!(),
        }
    }

    pub async fn delete_sink(&self, app_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => app.delete_sink(sink_id).await,
            None => app_not_found_err!(),
        }
    }
}

impl AppManager {
    pub async fn add_source_ref(
        &self,
        app_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == *app_id)
        {
            Some(app) => app.add_source_ref(source_id, rule_id).await,
            None => app_not_found_err!(),
        }
    }

    pub async fn get_source_rx(
        &self,
        app_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == *app_id)
        {
            Some(app) => app.get_source_rx(source_id, rule_id).await,
            None => app_not_found_err!(),
        }
    }

    pub async fn del_source_rx(
        &self,
        app_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == *app_id)
        {
            Some(app) => app.del_source_rx(source_id, rule_id).await,
            None => app_not_found_err!(),
        }
    }

    pub async fn del_source_ref(
        &self,
        app_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == *app_id)
        {
            Some(app) => app.del_source_ref(source_id, rule_id).await,
            None => app_not_found_err!(),
        }
    }

    pub async fn add_sink_ref(
        &self,
        app_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == *app_id)
        {
            Some(app) => app.add_sink_ref(sink_id, rule_id).await,
            None => app_not_found_err!(),
        }
    }

    pub async fn get_sink_tx(
        &self,
        app_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == *app_id)
        {
            Some(app) => app.get_sink_tx(sink_id, rule_id).await,
            None => app_not_found_err!(),
        }
    }

    pub async fn del_sink_tx(
        &self,
        app_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == *app_id)
        {
            Some(app) => app.del_sink_tx(sink_id, rule_id).await,
            None => app_not_found_err!(),
        }
    }

    pub async fn del_sink_ref(
        &self,
        app_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == *app_id)
        {
            Some(app) => app.del_sink_ref(sink_id, rule_id).await,
            None => app_not_found_err!(),
        }
    }
}

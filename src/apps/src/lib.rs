use std::{str::FromStr, sync::LazyLock, vec};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc, RwLock};
use types::{
    apps::{
        AppConf, AppType, CreateUpdateAppReq, QueryParams, SearchAppsItemResp, SearchAppsResp,
        Summary,
    },
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp,
};
use uuid::Uuid;

mod http_client;
mod log;
mod mqtt_client;

#[async_trait]
pub trait App: Send + Sync {
    fn get_id(&self) -> &Uuid;
    fn check_duplicate(&self, req: &CreateUpdateAppReq) -> HaliaResult<()>;
    async fn search(&self) -> SearchAppsItemResp;
    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()>;
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
    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::read_apps().await {
            Ok(datas) => {
                for data in datas {
                    let items: Vec<&str> = data.split(persistence::DELIMITER).collect();
                    assert_eq!(items.len(), 3);

                    let app_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdateAppReq = serde_json::from_str(items[2])?;
                    self.create_app(app_id, req, false).await?;

                    let sources = persistence::read_sources(&app_id).await?;
                    for source_data in sources {
                        let items = source_data
                            .split(persistence::DELIMITER)
                            .collect::<Vec<&str>>();
                        assert_eq!(items.len(), 2);
                        let source_id = Uuid::from_str(items[0]).unwrap();
                        let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(items[1])?;
                        self.create_source(app_id, source_id, req, false).await?;
                    }

                    let sinks = persistence::read_sinks(&app_id).await?;
                    for sink_data in sinks {
                        let items = sink_data
                            .split(persistence::DELIMITER)
                            .collect::<Vec<&str>>();
                        assert_eq!(items.len(), 2);
                        let sink_id = Uuid::from_str(items[0]).unwrap();
                        let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(items[1])?;
                        self.create_sink(app_id, sink_id, req, false).await?;
                    }

                    match items[1] {
                        "0" => {}
                        "1" => self.start_app(app_id).await.unwrap(),
                        _ => panic!("文件已损坏"),
                    }
                }

                Ok(())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => {
                    persistence::init_apps().await?;
                    Ok(())
                }
                _ => return Err(e.into()),
            },
        }
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

            if app.common.err.is_some() {
                err_cnt += 1;
            } else {
                if app.common.on {
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
        persist: bool,
    ) -> HaliaResult<()> {
        for app in self.apps.read().await.iter() {
            app.check_duplicate(&req)?;
        }

        let data = serde_json::to_string(&req)?;
        let device = match req.app_type {
            AppType::MqttClient => mqtt_client::new(app_id, req.conf)?,
            AppType::HttpClient => http_client::new(app_id, req.conf)?,
            AppType::Log => log::new(app_id, req.conf)?,
        };

        if persist {
            persistence::create_app(&app_id, &data).await?;
        }
        self.apps.write().await.push(device);
        Ok(())
    }

    pub async fn search_apps(&self, pagination: Pagination, query: QueryParams) -> SearchAppsResp {
        let mut data = vec![];
        let mut total = 0;

        for app in self.apps.read().await.iter().rev() {
            let app = app.search().await;
            if let Some(app_type) = &query.app_type {
                if *app_type != app.common.app_type {
                    continue;
                }
            }

            if let Some(name) = &query.name {
                if !app.conf.base.name.contains(name) {
                    continue;
                }
            }
            if let Some(on) = &query.on {
                if app.common.on != *on {
                    continue;
                }
            }

            if let Some(err) = &query.err {
                if app.common.err.is_some() != *err {
                    continue;
                }
            }

            if total >= (pagination.page - 1) * pagination.size
                && total < pagination.page * pagination.size
            {
                data.push(app);
            }
            total += 1;
        }

        SearchAppsResp { total, data }
    }

    pub async fn update_app(&self, app_id: Uuid, req: CreateUpdateAppReq) -> HaliaResult<()> {
        for app in self.apps.read().await.iter() {
            if *app.get_id() != app_id {
                app.check_duplicate(&req)?;
            }
        }

        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => {
                let data = serde_json::to_string(&req)?;
                app.update(req.conf).await?;
                persistence::update_app_conf(app.get_id(), &data).await?;
                Ok(())
            }
            None => app_not_found_err!(),
        }
    }

    pub async fn start_app(&self, app_id: Uuid) -> HaliaResult<()> {
        match self
            .apps
            .write()
            .await
            .iter_mut()
            .find(|app| *app.get_id() == app_id)
        {
            Some(app) => {
                app.start().await?;
                persistence::update_app_status(app.get_id(), persistence::Status::Runing).await?;
                Ok(())
            }
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
            Some(app) => {
                app.stop().await?;
                persistence::update_app_status(app.get_id(), persistence::Status::Stopped).await?;
                Ok(())
            }
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
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
        persist: bool,
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
                app.create_source(source_id, req).await?;
                if persist {
                    persistence::create_source(app.get_id(), &source_id, &data).await?;
                }
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
            Some(app) => {
                let data = serde_json::to_string(&req)?;
                app.update_source(source_id, req).await?;
                persistence::update_source(app.get_id(), &source_id, &data).await?;
                Ok(())
            }
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
            Some(app) => {
                app.delete_source(source_id).await?;
                persistence::delete_source(app.get_id(), &source_id).await?;
                Ok(())
            }
            None => app_not_found_err!(),
        }
    }
}

impl AppManager {
    pub async fn create_sink(
        &self,
        app_id: Uuid,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
        persist: bool,
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
                app.create_sink(sink_id, req).await?;
                if persist {
                    persistence::create_sink(app.get_id(), &sink_id, &data).await?;
                }
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
            Some(app) => {
                let data = serde_json::to_string(&req)?;
                app.update_sink(sink_id, req).await?;
                persistence::update_sink(app.get_id(), &sink_id, &data).await?;
                Ok(())
            }
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
            Some(app) => {
                app.delete_sink(sink_id).await?;
                persistence::delete_sink(app.get_id(), &sink_id).await?;
                Ok(())
            }
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

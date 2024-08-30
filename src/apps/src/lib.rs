use std::{str::FromStr, sync::Arc, vec};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use sqlx::AnyPool;
use tokio::sync::{broadcast, mpsc, RwLock};
use types::{
    apps::{
        AppConf, AppType, CreateUpdateAppReq, QueryParams, SearchAppsItemResp, SearchAppsResp,
        Summary,
    },
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksInfoResp,
    SearchSourcesOrSinksResp,
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
    async fn search_source(&self, source_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp>;
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
    async fn search_sink(&self, sink_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp>;
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

pub async fn load_from_persistence(
    pool: &Arc<AnyPool>,
) -> HaliaResult<Arc<RwLock<Vec<Box<dyn App>>>>> {
    let db_apps = persistence::app::read_apps(pool).await?;
    let apps: Arc<RwLock<Vec<Box<dyn App>>>> = Arc::new(RwLock::new(vec![]));
    for db_app in db_apps {
        let app_id = Uuid::from_str(&db_app.id).unwrap();

        let db_sources = persistence::source::read_sources(pool, &app_id).await?;
        let db_sinks = persistence::sink::read_sinks(pool, &app_id).await?;
        create_app(pool, &apps, app_id, db_app.conf, false).await?;

        for db_source in db_sources {
            create_source(
                pool,
                &apps,
                app_id,
                Uuid::from_str(&db_source.id).unwrap(),
                db_source.conf,
                false,
            )
            .await?;
        }

        for db_sink in db_sinks {
            create_sink(
                pool,
                &apps,
                app_id,
                Uuid::from_str(&db_sink.id).unwrap(),
                db_sink.conf,
                false,
            )
            .await?;
        }

        if db_app.status == 1 {
            start_app(pool, &apps, app_id).await?;
        }
    }

    Ok(apps)
}

pub async fn get_summary(apps: &Arc<RwLock<Vec<Box<dyn App>>>>) -> Summary {
    let mut total = 0;
    let mut running_cnt = 0;
    let mut err_cnt = 0;
    let mut off_cnt = 0;
    for app in apps.read().await.iter().rev() {
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
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateAppReq = serde_json::from_str(&body)?;
    for app in apps.read().await.iter() {
        app.check_duplicate(&req)?;
    }

    let app = match req.app_type {
        AppType::MqttClient => mqtt_client::new(app_id, req.conf)?,
        AppType::HttpClient => http_client::new(app_id, req.conf)?,
        AppType::Log => log::new(app_id, req.conf)?,
    };

    if persist {
        persistence::app::create_app(pool, &app_id, body).await?;
    }
    apps.write().await.push(app);
    Ok(())
}

pub async fn search_apps(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    pagination: Pagination,
    query: QueryParams,
) -> SearchAppsResp {
    let mut data = vec![];
    let mut total = 0;

    for app in apps.read().await.iter().rev() {
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

pub async fn update_app(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateAppReq = serde_json::from_str(&body)?;

    for app in apps.read().await.iter() {
        if *app.get_id() != app_id {
            app.check_duplicate(&req)?;
        }
    }

    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.update(req.conf).await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::app::update_app_conf(pool, &app_id, body).await?;

    Ok(())
}

pub async fn start_app(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.start().await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::app::update_app_status(pool, &app_id, true).await?;

    Ok(())
}

pub async fn stop_app(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.stop().await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::app::update_app_status(pool, &app_id, false).await?;
    Ok(())
}

pub async fn delete_app(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.delete().await?,
        None => return Err(HaliaError::NotFound),
    }

    apps.write().await.retain(|app| *app.get_id() != app_id);
    persistence::app::delete_app(pool, &app_id).await?;
    Ok(())
}

pub async fn create_source(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    source_id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.create_source(source_id, req).await?,
        None => return Err(HaliaError::NotFound),
    }

    if persist {
        persistence::source::create_source(pool, &app_id, &source_id, body).await?;
    }

    Ok(())
}

pub async fn search_sources(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    match apps.read().await.iter().find(|app| *app.get_id() == app_id) {
        Some(app) => Ok(app.search_sources(pagination, query).await),
        None => Err(HaliaError::NotFound),
    }
}

pub async fn update_source(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    source_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.update_source(source_id, req).await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::source::update_source(pool, &source_id, body).await?;
    Ok(())
}

pub async fn delete_source(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    source_id: Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.delete_source(source_id).await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::source::delete_source(pool, &source_id).await?;
    Ok(())
}

pub async fn add_source_ref(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == *app_id)
    {
        Some(app) => app.add_source_ref(source_id, rule_id).await,
        None => Err(HaliaError::NotFound),
    }
}

pub async fn get_source_rx(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == *app_id)
    {
        Some(app) => app.get_source_rx(source_id, rule_id).await,
        None => Err(HaliaError::NotFound),
    }
}

pub async fn del_source_rx(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == *app_id)
    {
        Some(app) => app.del_source_rx(source_id, rule_id).await,
        None => Err(HaliaError::NotFound),
    }
}

pub async fn del_source_ref(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == *app_id)
    {
        Some(app) => app.del_source_ref(source_id, rule_id).await,
        None => Err(HaliaError::NotFound),
    }
}

pub async fn create_sink(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    sink_id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.create_sink(sink_id, req).await?,
        None => return Err(HaliaError::NotFound),
    }

    if persist {
        persistence::sink::create_sink(pool, &app_id, &sink_id, body).await?;
    }

    Ok(())
}

pub async fn search_sinks(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    match apps.read().await.iter().find(|app| *app.get_id() == app_id) {
        Some(device) => Ok(device.search_sinks(pagination, query).await),
        None => Err(HaliaError::NotFound),
    }
}

pub async fn update_sink(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    sink_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.update_sink(sink_id, req).await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::sink::update_sink(pool, &sink_id, body).await?;
    Ok(())
}

pub async fn delete_sink(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    sink_id: Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == app_id)
    {
        Some(app) => app.delete_sink(sink_id).await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::sink::delete_sink(pool, &sink_id).await?;

    Ok(())
}

pub async fn add_sink_ref(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == *app_id)
    {
        Some(app) => app.add_sink_ref(sink_id, rule_id).await,
        None => Err(HaliaError::NotFound),
    }
}

pub async fn get_sink_tx(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == *app_id)
    {
        Some(app) => app.get_sink_tx(sink_id, rule_id).await,
        None => Err(HaliaError::NotFound),
    }
}

pub async fn del_sink_tx(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == *app_id)
    {
        Some(app) => app.del_sink_tx(sink_id, rule_id).await,
        None => Err(HaliaError::NotFound),
    }
}

pub async fn del_sink_ref(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match apps
        .write()
        .await
        .iter_mut()
        .find(|app| *app.get_id() == *app_id)
    {
        Some(app) => app.del_sink_ref(sink_id, rule_id).await,
        None => Err(HaliaError::NotFound),
    }
}

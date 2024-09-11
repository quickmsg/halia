use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
    vec,
};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    storage,
};
use message::MessageBatch;
use sqlx::AnyPool;
use tokio::sync::{broadcast, mpsc, RwLock};
use types::{
    apps::{
        AppConf, AppType, CreateUpdateAppReq, QueryParams, QueryRuleInfo, SearchAppsItemResp,
        SearchAppsResp, SearchRuleInfo, Summary,
    },
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksInfoResp,
    SearchSourcesOrSinksResp,
};
use uuid::Uuid;

mod http_client;
mod mqtt_client;

static APP_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static APP_ON_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static APP_RUNNING_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

fn get_app_count() -> usize {
    APP_COUNT.load(Ordering::SeqCst)
}

fn add_app_count() {
    APP_COUNT.fetch_add(1, Ordering::SeqCst);
}

fn sub_app_count() {
    APP_COUNT.fetch_sub(1, Ordering::SeqCst);
}

pub(crate) fn get_app_on_count() -> usize {
    APP_ON_COUNT.load(Ordering::SeqCst)
}

pub(crate) fn add_app_on_count() {
    APP_ON_COUNT.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn sub_app_on_count() {
    APP_ON_COUNT.fetch_sub(1, Ordering::SeqCst);
}

pub(crate) fn get_app_running_count() -> usize {
    APP_RUNNING_COUNT.load(Ordering::SeqCst)
}

pub(crate) fn add_app_running_count() {
    APP_RUNNING_COUNT.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn sub_app_running_count() {
    APP_RUNNING_COUNT.fetch_sub(1, Ordering::SeqCst);
}

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
    let db_apps = storage::app::read_apps(pool).await?;
    let apps: Arc<RwLock<Vec<Box<dyn App>>>> = Arc::new(RwLock::new(vec![]));
    for db_app in db_apps {
        let app_id = Uuid::from_str(&db_app.id).unwrap();

        let db_sources = storage::source::read_sources(pool, &app_id).await?;
        let db_sinks = storage::sink::read_sinks(pool, &app_id).await?;
        create_app(pool, &apps, app_id, db_app.conf, false).await?;

        for db_source in db_sources {
            // create_source(
            //     pool,
            //     &apps,
            //     app_id,
            //     Uuid::from_str(&db_source.id).unwrap(),
            //     db_source.conf,
            //     false,
            // )
            // .await?;
            todo!()
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

pub async fn get_summary() -> Summary {
    Summary {
        total: get_app_count(),
        on: get_app_on_count(),
        running: get_app_running_count(),
    }
}

pub async fn get_rule_info(
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    query: QueryRuleInfo,
) -> HaliaResult<SearchRuleInfo> {
    match apps
        .read()
        .await
        .iter()
        .find(|app| *app.get_id() == query.app_id)
    {
        Some(app) => {
            let app_info = app.search().await;
            match (query.source_id, query.sink_id) {
                (Some(source_id), None) => {
                    let source_info = app.search_source(&source_id).await?;
                    Ok(SearchRuleInfo {
                        app: app_info,
                        source: Some(source_info),
                        sink: None,
                    })
                }
                (None, Some(sink_id)) => {
                    let sink_info = app.search_sink(&sink_id).await?;
                    Ok(SearchRuleInfo {
                        app: app_info,
                        source: None,
                        sink: Some(sink_info),
                    })
                }
                _ => return Err(HaliaError::Common("查询id错误".to_owned())),
            }
        }
        None => Err(HaliaError::NotFound),
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
    };

    add_app_count();
    if persist {
        storage::app::create_app(pool, &app_id, body).await?;
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

    storage::app::update_app_conf(pool, &app_id, body).await?;

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

    storage::app::update_app_status(pool, &app_id, true).await?;

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

    storage::app::update_app_status(pool, &app_id, false).await?;
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

    sub_app_count();
    apps.write().await.retain(|app| *app.get_id() != app_id);
    storage::app::delete_app(pool, &app_id).await?;
    Ok(())
}

pub async fn create_source(
    pool: &Arc<AnyPool>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    app_id: Uuid,
    source_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
    persist: bool,
) -> HaliaResult<()> {
    // let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
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
        todo!()
        // storage::source::create_source(pool, &app_id, &source_id, req).await?;
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

    // storage::source::update_source(pool, &source_id, body).await?;
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

    storage::source::delete_source(pool, &source_id).await?;
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
        // storage::sink::create_sink(pool, &app_id, &sink_id, body).await?;
        todo!()
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

    storage::sink::update_sink(pool, &sink_id, body).await?;
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

    storage::sink::delete_sink(pool, &sink_id).await?;

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

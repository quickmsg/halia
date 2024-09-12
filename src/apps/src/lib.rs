use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    storage,
};
use dashmap::DashMap;
use message::MessageBatch;
use sqlx::AnyPool;
use tokio::sync::{broadcast, mpsc};
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
    fn check_duplicate(&self, req: &CreateUpdateAppReq) -> HaliaResult<()>;
    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()>;
    async fn stop(&mut self) -> HaliaResult<()>;

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    // async fn search_sources(
    //     &self,
    //     pagination: Pagination,
    //     query: QueryParams,
    // ) -> SearchSourcesOrSinksResp;
    // async fn read_source(&self, source_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp>;
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
    // async fn search_sinks(
    //     &self,
    //     pagination: Pagination,
    //     query: QueryParams,
    // ) -> SearchSourcesOrSinksResp;
    // async fn read_sink(&self, sink_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp>;
    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()>;

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>>;
    async fn get_sink_tx(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>>;
}

pub async fn load_from_storage(
    storage: &Arc<AnyPool>,
) -> HaliaResult<Arc<DashMap<Uuid, Box<dyn App>>>> {
    let count = storage::app::count_all(storage).await?;
    APP_COUNT.store(count, Ordering::SeqCst);

    let db_apps = storage::app::read_on(storage).await?;
    let apps: Arc<DashMap<Uuid, Box<dyn App>>> = Arc::new(DashMap::new());
    for db_app in db_apps {
        let app_id = Uuid::from_str(&db_app.id).unwrap();
        start_app(storage, &apps, app_id).await.unwrap();
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
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    query: QueryRuleInfo,
) -> HaliaResult<SearchRuleInfo> {
    todo!()
    // match apps
    //     .read()
    //     .await
    //     .iter()
    //     .find(|app| *app.get_id() == query.app_id)
    // {
    //     Some(app) => {
    //         let app_info = app.search().await;
    //         match (query.source_id, query.sink_id) {
    //             (Some(source_id), None) => {
    //                 let source_info = app.search_source(&source_id).await?;
    //                 Ok(SearchRuleInfo {
    //                     app: app_info,
    //                     source: Some(source_info),
    //                     sink: None,
    //                 })
    //             }
    //             (None, Some(sink_id)) => {
    //                 let sink_info = app.search_sink(&sink_id).await?;
    //                 Ok(SearchRuleInfo {
    //                     app: app_info,
    //                     source: None,
    //                     sink: Some(sink_info),
    //                 })
    //             }
    //             _ => return Err(HaliaError::Common("查询id错误".to_owned())),
    //         }
    //     }
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn create_app(
    storage: &Arc<AnyPool>,
    app_id: Uuid,
    req: CreateUpdateAppReq,
) -> HaliaResult<()> {
    // for app in apps.read().await.iter() {
    //     app.check_duplicate(&req)?;
    // }

    // TODO 验证配置
    // let app = match req.app_type {
    //     AppType::MqttClient => mqtt_client::new(app_id, req.conf)?,
    //     AppType::HttpClient => http_client::new(app_id, req.conf)?,
    // };

    add_app_count();
    storage::app::insert(storage, &app_id, req).await?;
    Ok(())
}

pub async fn search_apps(
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    pagination: Pagination,
    query: QueryParams,
) -> SearchAppsResp {
    // let mut data = vec![];
    // let mut total = 0;

    // for app in apps.read().await.iter().rev() {
    //     let app = app.search().await;
    //     if let Some(app_type) = &query.app_type {
    //         if *app_type != app.common.app_type {
    //             continue;
    //         }
    //     }

    //     if let Some(name) = &query.name {
    //         if !app.conf.base.name.contains(name) {
    //             continue;
    //         }
    //     }
    //     if let Some(on) = &query.on {
    //         if app.common.on != *on {
    //             continue;
    //         }
    //     }

    //     if let Some(err) = &query.err {
    //         if app.common.err.is_some() != *err {
    //             continue;
    //         }
    //     }

    //     if total >= (pagination.page - 1) * pagination.size
    //         && total < pagination.page * pagination.size
    //     {
    //         data.push(app);
    //     }
    //     total += 1;
    // }

    // SearchAppsResp { total, data }
    todo!()
}

pub async fn update_app(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
    req: CreateUpdateAppReq,
) -> HaliaResult<()> {
    // for app in apps.read().await.iter() {
    //     if *app.get_id() != app_id {
    //         app.check_duplicate(&req)?;
    //     }
    // }
    if let Some(app) = apps.get_mut(&app_id) {
        // let db_app = storage::app::read_by_id(storage, &app_id).await?;
        // app.check_duplicate(&req)?;
    }

    storage::app::update(storage, &app_id, req).await?;

    Ok(())
}

pub async fn start_app(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
) -> HaliaResult<()> {
    if apps.contains_key(&app_id) {
        return Ok(());
    }

    // 创建事件
    // todo 从数据库读取配置

    storage::app::update_status(storage, &app_id, true).await?;
    add_app_on_count();

    Ok(())
}

pub async fn stop_app(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
) -> HaliaResult<()> {
    if !apps.contains_key(&app_id) {
        return Ok(());
    }

    apps.get_mut(&app_id).unwrap().stop().await?;

    apps.remove(&app_id);

    storage::app::update_status(storage, &app_id, false).await?;
    Ok(())
}

pub async fn delete_app(
    pool: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
) -> HaliaResult<()> {
    if apps.contains_key(&app_id) {
        return Err(HaliaError::Common("请先停止应用".to_owned()));
    }

    // 删除事件 测试是否可以删除

    sub_app_count();
    storage::app::delete(pool, &app_id).await?;
    Ok(())
}

pub async fn create_source(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    let source_id = Uuid::new_v4();

    storage::source_or_sink::create(
        storage,
        &app_id,
        &source_id,
        storage::source_or_sink::Type::Source,
        req,
    )
    .await?;

    // if let Some(mut app) = apps.get_mut(&app_id) {
    //     app.create_source(source_id, req).await?;
    // }
    // todo

    Ok(())
}

pub async fn search_sources(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    todo!()
}

pub async fn update_source(
    pool: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
    source_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    todo!()
}

pub async fn delete_source(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
    source_id: Uuid,
) -> HaliaResult<()> {
    let rule_ref_cnt =
        storage::rule_ref::count_active_cnt_by_resource_id(storage, &source_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::Common("请先删除关联规则".to_owned()));
    }

    if let Some(mut app) = apps.get_mut(&app_id) {
        app.delete_source(source_id).await?;
    }

    storage::source_or_sink::delete(storage, &source_id).await?;
    Ok(())
}

pub async fn get_source_rx(
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: &Uuid,
    source_id: &Uuid,
) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
    apps.get_mut(&app_id)
        .ok_or(HaliaError::Stopped)?
        .get_source_rx(source_id)
        .await
}

pub async fn create_sink(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    let sink_id = Uuid::new_v4();
    if let Some(mut app) = apps.get_mut(&app_id) {
        app.create_sink(sink_id, req).await?;
    }

    // storage::source_or_sink::create(
    //     storage,
    //     &app_id,
    //     &sink_id,
    //     storage::source_or_sink::Type::Sink,
    //     req,
    // )
    // .await?;

    Ok(())
}

pub async fn search_sinks(
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    todo!()
    // match apps.read().await.iter().find(|app| *app.get_id() == app_id) {
    //     Some(device) => Ok(device.search_sinks(pagination, query).await),
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn update_sink(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
    sink_id: Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    todo!()
    // let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    // match apps
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|app| *app.get_id() == app_id)
    // {
    //     Some(app) => app.update_sink(sink_id, req).await?,
    //     None => return Err(HaliaError::NotFound),
    // }

    // // storage::sink::update_sink(pool, &sink_id, req).await?;
    // Ok(())
}

pub async fn delete_sink(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
    sink_id: Uuid,
) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule_ref::count_cnt_by_resource_id(storage, &sink_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    if let Some(mut app) = apps.get_mut(&app_id) {
        app.delete_sink(sink_id).await?;
    }

    storage::source_or_sink::delete(storage, &sink_id).await?;

    Ok(())
}

pub async fn get_sink_tx(
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: &Uuid,
    sink_id: &Uuid,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    apps.get_mut(&app_id)
        .ok_or(HaliaError::Stopped)?
        .get_sink_tx(sink_id)
        .await
}

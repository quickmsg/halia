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
        AppConf, AppType, CreateUpdateAppReq, QueryParams, QueryRuleInfo, SearchAppsItemCommon,
        SearchAppsItemConf, SearchAppsItemResp, SearchAppsResp, SearchRuleInfo, Summary,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksInfoResp,
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
    let apps: Arc<DashMap<Uuid, Box<dyn App>>> = Arc::new(DashMap::new());

    let count = storage::app::count(storage).await?;
    APP_COUNT.store(count, Ordering::SeqCst);

    let db_apps = storage::app::read_on_all(storage).await?;

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
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    query: QueryRuleInfo,
) -> HaliaResult<SearchRuleInfo> {
    let db_app = storage::app::read_one(storage, &query.app_id).await?;

    let app_resp = transer_db_app_to_resp(storage, db_app, &query.app_id).await?;
    match (query.source_id, query.sink_id) {
        (Some(source_id), None) => {
            let db_source = storage::source_or_sink::read_one(storage, &source_id).await?;
            Ok(SearchRuleInfo {
                app: app_resp,
                source: Some(SearchSourcesOrSinksInfoResp {
                    id: db_source.id,
                    conf: CreateUpdateSourceOrSinkReq {
                        base: BaseConf {
                            name: db_source.name,
                            desc: db_source.desc,
                        },
                        ext: serde_json::from_str(&db_source.conf)?,
                    },
                }),
                sink: None,
            })
        }
        (None, Some(sink_id)) => {
            let db_sink = storage::source_or_sink::read_one(storage, &sink_id).await?;
            Ok(SearchRuleInfo {
                app: app_resp,
                source: Some(SearchSourcesOrSinksInfoResp {
                    id: db_sink.id,
                    conf: CreateUpdateSourceOrSinkReq {
                        base: BaseConf {
                            name: db_sink.name,
                            desc: db_sink.desc,
                        },
                        ext: serde_json::from_str(&db_sink.conf)?,
                    },
                }),
                sink: None,
            })
        }
        _ => {
            return Err(HaliaError::Common(
                "查询source_id或sink_id参数错误！".to_string(),
            ))
        }
    }
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
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchAppsResp> {
    let (count, db_apps) = storage::app::query(storage, pagination, query).await?;

    let mut apps_resp = Vec::with_capacity(db_apps.len());
    for db_app in db_apps {
        let id = Uuid::from_str(&db_app.id).unwrap();
        apps_resp.push(transer_db_app_to_resp(storage, db_app, &id).await?);
    }

    Ok(SearchAppsResp {
        total: count,
        data: apps_resp,
    })
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

    let db_app = storage::app::read_one(storage, &app_id).await?;
    let app_type = AppType::try_from(db_app.typ)?;
    let app_id = Uuid::from_str(&db_app.id).unwrap();
    let app_conf = AppConf {
        base: BaseConf {
            name: db_app.name,
            desc: db_app.desc,
        },
        ext: serde_json::from_str(&db_app.conf)?,
    };

    let app = match app_type {
        AppType::MqttClient => mqtt_client::new(app_id, app_conf)?,
        AppType::HttpClient => http_client::new(app_id, app_conf)?,
    };
    apps.insert(app_id, app);

    let mut app = apps.get_mut(&app_id).unwrap();
    let db_sources = storage::source_or_sink::read_all_by_parent_id(
        storage,
        &app_id,
        storage::source_or_sink::Type::Source,
    )
    .await?;
    for db_source in db_sources {
        let conf: serde_json::Value = serde_json::from_str(&db_source.conf).unwrap();
        app.create_source(Uuid::from_str(&db_source.id).unwrap(), conf)
            .await?;
    }

    let db_sinks = storage::source_or_sink::read_all_by_parent_id(
        storage,
        &app_id,
        storage::source_or_sink::Type::Sink,
    )
    .await?;
    for db_sink in db_sinks {
        let conf: serde_json::Value = serde_json::from_str(&db_sink.conf).unwrap();
        app.create_sink(Uuid::from_str(&db_sink.id).unwrap(), conf)
            .await?;
    }

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

    let active_rule_ref_cnt =
        storage::rule_ref::count_active_cnt_by_resource_id(storage, &app_id).await?;
    if active_rule_ref_cnt > 0 {
        return Err(HaliaError::StopActiveRefing);
    }

    apps.get_mut(&app_id).unwrap().stop().await?;

    apps.remove(&app_id);
    storage::app::update_status(storage, &app_id, false).await?;
    sub_app_on_count();
    Ok(())
}

pub async fn delete_app(
    storage: &Arc<AnyPool>,
    apps: &Arc<DashMap<Uuid, Box<dyn App>>>,
    app_id: Uuid,
) -> HaliaResult<()> {
    if apps.contains_key(&app_id) {
        return Err(HaliaError::Common("请先停止应用".to_owned()));
    }

    let cnt = storage::rule_ref::count_cnt_by_parent_id(storage, &app_id).await?;
    if cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    // 删除事件 测试是否可以删除

    sub_app_count();
    storage::app::delete(storage, &app_id).await?;
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

    storage::source_or_sink::delete(storage, &sink_id).await?;
    if let Some(mut app) = apps.get_mut(&app_id) {
        app.delete_sink(sink_id).await?;
    }

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

async fn transer_db_app_to_resp(
    storage: &Arc<AnyPool>,
    db_app: storage::app::App,
    id: &Uuid,
) -> HaliaResult<SearchAppsItemResp> {
    Ok(SearchAppsItemResp {
        common: SearchAppsItemCommon {
            id: db_app.id,
            typ: db_app.typ,
            on: db_app.status == 1,
            source_cnt: storage::source_or_sink::count_by_parent_id(
                storage,
                id,
                storage::source_or_sink::Type::Source,
            )
            .await?,
            sink_cnt: storage::source_or_sink::count_by_parent_id(
                storage,
                id,
                storage::source_or_sink::Type::Sink,
            )
            .await?,
            // TODO
            memory_info: None,
        },
        conf: SearchAppsItemConf {
            base: BaseConf {
                name: db_app.name,
                desc: db_app.desc,
            },
            ext: serde_json::from_str(&db_app.conf)?,
        },
    })
}

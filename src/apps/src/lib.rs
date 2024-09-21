use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    storage,
};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};
use types::{
    apps::{
        AppConf, AppType, CreateUpdateAppReq, QueryParams, QueryRuleInfo, SearchAppsItemCommon,
        SearchAppsItemConf, SearchAppsItemResp, SearchAppsResp, SearchRuleInfo, Summary,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams, RuleRef,
    SearchSourcesOrSinksInfoResp, SearchSourcesOrSinksItemResp, SearchSourcesOrSinksResp,
};

mod http_client;
mod mqtt_client;

static GLOBAL_APP_MANAGER: LazyLock<DashMap<String, Box<dyn App>>> =
    LazyLock::new(|| DashMap::new());

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

pub(crate) fn _add_app_running_count() {
    APP_RUNNING_COUNT.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn _sub_app_running_count() {
    APP_RUNNING_COUNT.fetch_sub(1, Ordering::SeqCst);
}

#[async_trait]
pub trait App: Send + Sync {
    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()>;
    async fn stop(&mut self) -> HaliaResult<()>;

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn update_source(
        &mut self,
        source_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn delete_source(&mut self, source_id: String) -> HaliaResult<()>;

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()>;
    async fn update_sink(
        &mut self,
        sink_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: String) -> HaliaResult<()>;

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>>;
    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>>;
}

pub async fn load_from_storage() -> HaliaResult<()> {
    let count = storage::app::count().await?;
    APP_COUNT.store(count, Ordering::SeqCst);

    let db_apps = storage::app::read_on_all().await?;

    for db_app in db_apps {
        start_app(db_app.id).await.unwrap();
    }

    Ok(())
}

pub async fn get_summary() -> Summary {
    Summary {
        total: get_app_count(),
        on: get_app_on_count(),
        running: get_app_running_count(),
    }
}

pub async fn get_rule_info(query: QueryRuleInfo) -> HaliaResult<SearchRuleInfo> {
    let db_app = storage::app::read_one(&query.app_id).await?;

    let app_resp = transer_db_app_to_resp(db_app).await?;
    match (query.source_id, query.sink_id) {
        (Some(source_id), None) => {
            let db_source = storage::source_or_sink::read_one(&source_id).await?;
            Ok(SearchRuleInfo {
                app: app_resp,
                source: Some(SearchSourcesOrSinksInfoResp {
                    id: db_source.id,
                    conf: CreateUpdateSourceOrSinkReq {
                        base: BaseConf {
                            name: db_source.name,
                            desc: db_source
                                .des
                                .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                        },
                        ext: serde_json::from_slice(&db_source.conf)?,
                    },
                }),
                sink: None,
            })
        }
        (None, Some(sink_id)) => {
            let db_sink = storage::source_or_sink::read_one(&sink_id).await?;
            Ok(SearchRuleInfo {
                app: app_resp,
                source: Some(SearchSourcesOrSinksInfoResp {
                    id: db_sink.id,
                    conf: CreateUpdateSourceOrSinkReq {
                        base: BaseConf {
                            name: db_sink.name,
                            desc: db_sink
                                .des
                                .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                        },
                        ext: serde_json::from_slice(&db_sink.conf)?,
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

pub async fn create_app(req: CreateUpdateAppReq) -> HaliaResult<()> {
    if storage::app::insert_name_exists(&req.conf.base.name).await? {
        return Err(HaliaError::NameExists);
    }

    let app_id = common::get_id();
    // for app in apps.read().await.iter() {
    //     app.check_duplicate(&req)?;
    // }

    // TODO 验证配置
    // let app = match req.app_type {
    //     AppType::MqttClient => mqtt_client::new(app_id, req.conf)?,
    //     AppType::HttpClient => http_client::new(app_id, req.conf)?,
    // };

    add_app_count();
    storage::app::insert(app_id, req).await?;
    Ok(())
}

pub async fn search_apps(
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchAppsResp> {
    let (count, db_apps) = storage::app::query(pagination, query).await?;

    let mut apps_resp = Vec::with_capacity(db_apps.len());
    for db_app in db_apps {
        apps_resp.push(transer_db_app_to_resp(db_app).await?);
    }

    Ok(SearchAppsResp {
        total: count,
        data: apps_resp,
    })
}

pub async fn update_app(app_id: String, req: CreateUpdateAppReq) -> HaliaResult<()> {
    if storage::app::update_name_exists(&app_id, &req.conf.base.name).await? {
        return Err(HaliaError::NameExists);
    }

    // for app in apps.read().await.iter() {
    //     if *app.get_id() != app_id {
    //         app.check_duplicate(&req)?;
    //     }
    // }
    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let conf: AppConf = serde_json::from_value(req.conf.ext.clone())?;
        app.update(conf).await?;
        // let db_app = storage::app::read_by_id(storage, &app_id).await?;
        // app.check_duplicate(&req)?;
    }

    storage::app::update(app_id, req).await?;

    Ok(())
}

pub async fn start_app(app_id: String) -> HaliaResult<()> {
    if GLOBAL_APP_MANAGER.contains_key(&app_id) {
        return Ok(());
    }

    let db_app = storage::app::read_one(&app_id).await?;
    let app_type = AppType::try_from(db_app.typ)?;
    let app_conf = AppConf {
        base: BaseConf {
            name: db_app.name,
            desc: db_app.desc,
        },
        ext: serde_json::from_str(&db_app.conf)?,
    };

    let app = match app_type {
        AppType::MqttClient => mqtt_client::new(app_id.clone(), app_conf)?,
        AppType::HttpClient => http_client::new(app_id.clone(), app_conf)?,
    };
    GLOBAL_APP_MANAGER.insert(app_id.clone(), app);

    let mut app = GLOBAL_APP_MANAGER.get_mut(&app_id).unwrap();
    let db_sources = storage::source_or_sink::read_all_by_parent_id(
        &app_id,
        storage::source_or_sink::Type::Source,
    )
    .await?;
    for db_source in db_sources {
        let conf: serde_json::Value = serde_json::from_slice(&db_source.conf).unwrap();
        app.create_source(db_source.id, conf).await?;
    }

    let db_sinks = storage::source_or_sink::read_all_by_parent_id(
        &app_id,
        storage::source_or_sink::Type::Sink,
    )
    .await?;
    for db_sink in db_sinks {
        let conf: serde_json::Value = serde_json::from_slice(&db_sink.conf).unwrap();
        app.create_sink(db_sink.id, conf).await?;
    }

    storage::app::update_status(&app_id, true).await?;
    add_app_on_count();

    Ok(())
}

pub async fn stop_app(app_id: String) -> HaliaResult<()> {
    if !GLOBAL_APP_MANAGER.contains_key(&app_id) {
        return Ok(());
    }

    let active_rule_ref_cnt = storage::rule_ref::count_active_cnt_by_resource_id(&app_id).await?;
    if active_rule_ref_cnt > 0 {
        return Err(HaliaError::StopActiveRefing);
    }

    GLOBAL_APP_MANAGER.get_mut(&app_id).unwrap().stop().await?;
    GLOBAL_APP_MANAGER.remove(&app_id);
    storage::app::update_status(&app_id, false).await?;
    sub_app_on_count();
    Ok(())
}

pub async fn delete_app(app_id: String) -> HaliaResult<()> {
    if GLOBAL_APP_MANAGER.contains_key(&app_id) {
        return Err(HaliaError::DeleteRefing);
    }

    let cnt = storage::rule_ref::count_cnt_by_parent_id(&app_id).await?;
    if cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    // 删除事件

    sub_app_count();
    storage::app::delete(&app_id).await?;
    Ok(())
}

pub async fn create_source(app_id: String, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
    if storage::source_or_sink::insert_name_exists(
        &app_id,
        storage::source_or_sink::Type::Source,
        &req.base.name,
    )
    .await?
    {
        return Err(HaliaError::NameExists);
    }

    let source_id = common::get_id();

    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let conf = req.ext.clone();
        app.create_source(source_id.clone(), conf).await?;
    }

    storage::source_or_sink::insert(
        &app_id,
        &source_id,
        storage::source_or_sink::Type::Source,
        req,
    )
    .await?;

    Ok(())
}

pub async fn search_sources(
    app_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sources) = storage::source_or_sink::query_by_parent_id(
        &app_id,
        storage::source_or_sink::Type::Source,
        pagination,
        query,
    )
    .await?;
    let mut data = Vec::with_capacity(db_sources.len());
    for db_source in db_sources {
        let rule_ref = RuleRef {
            rule_ref_cnt: storage::rule_ref::count_cnt_by_resource_id(&db_source.id).await?,
            rule_active_ref_cnt: storage::rule_ref::count_active_cnt_by_resource_id(&db_source.id)
                .await?,
        };
        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id: db_source.id,
                conf: CreateUpdateSourceOrSinkReq {
                    base: BaseConf {
                        name: db_source.name,
                        desc: db_source
                            .des
                            .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                    },
                    ext: serde_json::from_slice(&db_source.conf).unwrap(),
                },
            },
            rule_ref,
        });
    }

    Ok(SearchSourcesOrSinksResp { total: count, data })
}

pub async fn update_source(
    app_id: String,
    source_id: String,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    if storage::source_or_sink::update_name_exists(
        &app_id,
        storage::source_or_sink::Type::Source,
        &req.base.name,
        &source_id,
    )
    .await?
    {
        return Err(HaliaError::NameExists);
    }

    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let old_conf = storage::source_or_sink::read_conf(&source_id).await?;
        let new_conf = req.ext.clone();
        app.update_source(source_id.clone(), old_conf, new_conf)
            .await?;
    }
    storage::source_or_sink::update(&source_id, req).await?;

    Ok(())
}

pub async fn delete_source(app_id: String, source_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule_ref::count_active_cnt_by_resource_id(&source_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::Common("请先删除关联规则".to_owned()));
    }

    storage::source_or_sink::delete(&source_id).await?;

    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        app.delete_source(source_id).await?;
    }

    Ok(())
}

pub async fn get_source_rx(
    app_id: &String,
    source_id: &String,
) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
    GLOBAL_APP_MANAGER
        .get_mut(app_id)
        .ok_or(HaliaError::Stopped)?
        .get_source_rx(source_id)
        .await
}

pub async fn create_sink(app_id: String, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
    if storage::source_or_sink::insert_name_exists(
        &app_id,
        storage::source_or_sink::Type::Sink,
        &req.base.name,
    )
    .await?
    {
        return Err(HaliaError::NameExists);
    }

    let sink_id = common::get_id();
    storage::source_or_sink::insert(
        &app_id,
        &sink_id,
        storage::source_or_sink::Type::Sink,
        req.clone(),
    )
    .await?;

    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let conf = req.ext.clone();
        app.create_sink(sink_id, conf).await?;
    }

    Ok(())
}

pub async fn search_sinks(
    app_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sinks) = storage::source_or_sink::query_by_parent_id(
        &app_id,
        storage::source_or_sink::Type::Sink,
        pagination,
        query,
    )
    .await?;
    let mut data = Vec::with_capacity(db_sinks.len());
    for db_sink in db_sinks {
        let rule_ref = RuleRef {
            rule_ref_cnt: storage::rule_ref::count_cnt_by_resource_id(&db_sink.id).await?,
            rule_active_ref_cnt: storage::rule_ref::count_active_cnt_by_resource_id(&db_sink.id)
                .await?,
        };
        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id: db_sink.id,
                conf: CreateUpdateSourceOrSinkReq {
                    base: BaseConf {
                        name: db_sink.name,
                        desc: db_sink
                            .des
                            .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                    },
                    ext: serde_json::from_slice(&db_sink.conf).unwrap(),
                },
            },
            rule_ref,
        });
    }

    Ok(SearchSourcesOrSinksResp { total: count, data })
}

pub async fn update_sink(
    app_id: String,
    sink_id: String,
    req: CreateUpdateSourceOrSinkReq,
) -> HaliaResult<()> {
    if storage::source_or_sink::update_name_exists(
        &app_id,
        storage::source_or_sink::Type::Sink,
        &req.base.name,
        &sink_id,
    )
    .await?
    {
        return Err(HaliaError::NameExists);
    }

    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let old_conf = storage::source_or_sink::read_conf(&sink_id).await?;
        let new_conf = req.ext.clone();
        app.update_sink(sink_id.clone(), old_conf, new_conf).await?;
    }

    storage::source_or_sink::update(&sink_id, req.clone()).await?;

    Ok(())
}

pub async fn delete_sink(app_id: String, sink_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule_ref::count_cnt_by_resource_id(&sink_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    storage::source_or_sink::delete(&sink_id).await?;

    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        app.delete_sink(sink_id).await?;
    }

    Ok(())
}

pub async fn get_sink_tx(
    app_id: &String,
    sink_id: &String,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    GLOBAL_APP_MANAGER
        .get_mut(app_id)
        .ok_or(HaliaError::Stopped)?
        .get_sink_tx(sink_id)
        .await
}

async fn transer_db_app_to_resp(db_app: storage::app::App) -> HaliaResult<SearchAppsItemResp> {
    let source_cnt = storage::source_or_sink::count_by_parent_id(
        &db_app.id,
        storage::source_or_sink::Type::Source,
    )
    .await?;

    let sink_cnt = storage::source_or_sink::count_by_parent_id(
        &db_app.id,
        storage::source_or_sink::Type::Sink,
    )
    .await?;

    Ok(SearchAppsItemResp {
        common: SearchAppsItemCommon {
            id: db_app.id,
            typ: db_app.typ,
            on: db_app.status == 1,
            source_cnt,
            sink_cnt,
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

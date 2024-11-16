use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use tokio::sync::mpsc;
use types::{
    apps::{
        AppType, CreateAppReq, ListAppsItem, ListAppsResp, QueryParams, QueryRuleInfo, ReadAppResp,
        SearchRuleInfo, Summary, UpdateAppReq,
    },
    rules::{ListRulesItem, ListRulesResp},
    CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams, RuleRef,
    SearchSourcesOrSinksInfoResp, SearchSourcesOrSinksItemResp, SearchSourcesOrSinksResp, Status,
};

mod http;
mod influxdb_v1;
mod influxdb_v2;
mod kafka;
mod mqtt_client_ssl;
mod mqtt_v311;
mod mqtt_v50;
mod tdengine;

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

pub(crate) fn add_app_running_count() {
    APP_RUNNING_COUNT.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn sub_app_running_count() {
    APP_RUNNING_COUNT.fetch_sub(1, Ordering::SeqCst);
}

#[async_trait]
pub trait App: Send + Sync {
    async fn read_err(&self) -> Option<String>;
    async fn update(
        &mut self,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn stop(&mut self);

    async fn create_source(
        &mut self,
        _source_id: String,
        _conf: serde_json::Value,
    ) -> HaliaResult<()> {
        Err(HaliaError::NotSupportResource)
    }
    async fn update_source(
        &mut self,
        _source_id: String,
        _old_conf: serde_json::Value,
        _new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        Err(HaliaError::NotSupportResource)
    }
    async fn delete_source(&mut self, _source_id: String) -> HaliaResult<()> {
        Err(HaliaError::NotSupportResource)
    }
    async fn get_source_rxs(
        &self,
        _source_id: &String,
        _cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>> {
        Err(HaliaError::NotSupportResource)
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()>;
    async fn update_sink(
        &mut self,
        sink_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: String) -> HaliaResult<()>;
    async fn get_sink_txs(
        &self,
        sink_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>>;
}

pub async fn load_from_storage() -> HaliaResult<()> {
    let count = storage::app::count().await?;
    APP_COUNT.store(count, Ordering::SeqCst);

    let db_apps = storage::app::read_all_running().await?;

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
    let app = transer_db_app_to_resp(db_app).await?;
    match (query.source_id, query.sink_id) {
        (Some(source_id), None) => {
            let db_source = storage::app::source_sink::read_one(&source_id).await?;
            Ok(SearchRuleInfo {
                app,
                source: Some(SearchSourcesOrSinksInfoResp {
                    id: db_source.id,
                    conf: CreateUpdateSourceOrSinkReq {
                        name: db_source.name,
                        conf: db_source.conf,
                    },
                }),
                sink: None,
            })
        }
        (None, Some(sink_id)) => {
            let db_sink = storage::app::source_sink::read_one(&sink_id).await?;
            Ok(SearchRuleInfo {
                app,
                source: None,
                sink: Some(SearchSourcesOrSinksInfoResp {
                    id: db_sink.id,
                    conf: CreateUpdateSourceOrSinkReq {
                        name: db_sink.name,
                        conf: db_sink.conf,
                    },
                }),
            })
        }
        _ => {
            return Err(HaliaError::Common(
                "查询source_id或sink_id参数错误！".to_string(),
            ))
        }
    }
}

pub async fn create_app(req: CreateAppReq) -> HaliaResult<()> {
    match req.typ {
        AppType::MqttV311 => mqtt_v311::validate_conf(&req.conf)?,
        AppType::MqttV50 => mqtt_v50::validate_conf(&req.conf)?,
        AppType::Http => http::validate_conf(&req.conf)?,
        AppType::Kafka => kafka::validate_conf(&req.conf)?,
        AppType::InfluxdbV1 => influxdb_v1::validate_conf(&req.conf)?,
        AppType::InfluxdbV2 => influxdb_v2::validate_conf(&req.conf)?,
        AppType::Tdengine => tdengine::validate_conf(&req.conf)?,
    }

    let app_id = common::get_id();
    storage::app::insert(&app_id, req).await?;
    events::insert_create(types::events::ResourceType::App, &app_id).await;

    add_app_count();
    Ok(())
}

pub async fn list_apps(pagination: Pagination, query: QueryParams) -> HaliaResult<ListAppsResp> {
    let (count, db_apps) = storage::app::search(pagination, query).await?;

    let mut apps_resp = Vec::with_capacity(db_apps.len());
    for db_app in db_apps {
        apps_resp.push(transer_db_app_to_resp(db_app).await?);
    }

    Ok(ListAppsResp {
        count,
        list: apps_resp,
    })
}

pub async fn read_app(app_id: String) -> HaliaResult<ReadAppResp> {
    let db_app = storage::app::read_one(&app_id).await?;
    let (can_stop, can_delete, err) = get_info_by_status(&app_id, &db_app.status).await?;
    Ok(ReadAppResp {
        id: db_app.id,
        name: db_app.name,
        conf: db_app.conf,
        can_stop,
        can_delete,
        err,
    })
}

pub async fn update_app(app_id: String, req: UpdateAppReq) -> HaliaResult<()> {
    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let db_conf = storage::app::read_conf(&app_id).await?;
        app.update(db_conf, req.conf.clone()).await?;
    }

    storage::app::update_conf(app_id, req).await?;

    Ok(())
}

pub async fn start_app(app_id: String) -> HaliaResult<()> {
    if GLOBAL_APP_MANAGER.contains_key(&app_id) {
        return Ok(());
    }

    let db_app = storage::app::read_one(&app_id).await?;
    let app = match db_app.typ {
        AppType::MqttV311 => mqtt_v311::new(app_id.clone(), db_app.conf),
        AppType::MqttV50 => mqtt_v50::new(app_id.clone(), db_app.conf),
        AppType::Http => http::new(app_id.clone(), db_app.conf),
        AppType::Kafka => kafka::new(app_id.clone(), db_app.conf),
        AppType::InfluxdbV1 => influxdb_v1::new(app_id.clone(), db_app.conf),
        AppType::InfluxdbV2 => influxdb_v2::new(app_id.clone(), db_app.conf),
        AppType::Tdengine => tdengine::new(app_id.clone(), db_app.conf),
    };
    GLOBAL_APP_MANAGER.insert(app_id.clone(), app);

    let mut app = GLOBAL_APP_MANAGER.get_mut(&app_id).unwrap();
    let db_sources = storage::app::source_sink::read_all_sources_by_app_id(&app_id).await?;
    for db_source in db_sources {
        app.create_source(db_source.id, db_source.conf).await?;
    }

    let db_sinks = storage::app::source_sink::read_all_sinks_by_app_id(&app_id).await?;
    for db_sink in db_sinks {
        app.create_sink(db_sink.id, db_sink.conf).await?;
    }

    storage::app::update_status(&app_id, types::Status::Running).await?;
    add_app_on_count();

    Ok(())
}

pub async fn stop_app(app_id: String) -> HaliaResult<()> {
    if storage::rule::reference::count_active_cnt_by_parent_id(&app_id).await? > 0 {
        return Err(HaliaError::StopActiveRefing);
    }

    if let Some((_, mut app)) = GLOBAL_APP_MANAGER.remove(&app_id) {
        app.stop().await;
        sub_app_on_count();
        events::insert_stop(types::events::ResourceType::App, &app_id).await;
        storage::app::update_status(&app_id, types::Status::Stopped).await?;
    }

    Ok(())
}

pub async fn delete_app(app_id: String) -> HaliaResult<()> {
    if GLOBAL_APP_MANAGER.contains_key(&app_id) {
        return Err(HaliaError::DeleteRunning);
    }

    let cnt = storage::rule::reference::count_cnt_by_parent_id(&app_id).await?;
    if cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    events::insert_delete(types::events::ResourceType::App, &app_id).await;

    sub_app_count();
    storage::app::delete_by_id(&app_id).await?;
    Ok(())
}

pub async fn list_rules(
    app_id: String,
    pagination: Pagination,
    query: types::rules::QueryParams,
) -> HaliaResult<ListRulesResp> {
    let (count, db_rules) = storage::rule::search(pagination, query, Some(&app_id)).await?;
    let list = db_rules
        .into_iter()
        .map(|x| ListRulesItem {
            id: x.id,
            name: x.name,
            status: x.status,
        })
        .collect();
    Ok(ListRulesResp { count, list })
}

pub async fn create_source(app_id: String, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
    let typ: AppType = storage::app::read_type(&app_id).await?.try_into()?;
    let source_id = common::get_id();
    match typ {
        AppType::MqttV311 => mqtt_v311::process_source_conf(&source_id, &req.conf).await?,
        AppType::MqttV50 => mqtt_v50::validate_source_conf(&req.conf)?,
        AppType::Http => http::validate_source_conf(&req.conf)?,
        AppType::Kafka | AppType::InfluxdbV1 | AppType::InfluxdbV2 | AppType::Tdengine => {
            return Err(HaliaError::NotSupportResource)
        }
    }

    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let conf = req.conf.clone();
        app.create_source(source_id.clone(), conf).await?;
    }

    storage::app::source_sink::insert_source(&app_id, &source_id, req).await?;

    Ok(())
}

pub async fn search_sources(
    app_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sources) =
        storage::app::source_sink::query_sources_by_app_id(&app_id, pagination, query).await?;
    let mut data = Vec::with_capacity(db_sources.len());
    for db_source in db_sources {
        let rule_ref = RuleRef {
            rule_ref_cnt: storage::rule::reference::count_cnt_by_resource_id(&db_source.id).await?,
            rule_active_ref_cnt: storage::rule::reference::count_active_cnt_by_resource_id(
                &db_source.id,
            )
            .await?,
        };
        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id: db_source.id,
                conf: CreateUpdateSourceOrSinkReq {
                    name: db_source.name,
                    conf: db_source.conf,
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
    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let old_conf = storage::app::source_sink::read_conf(&source_id).await?;
        let new_conf = req.conf.clone();
        app.update_source(source_id.clone(), old_conf, new_conf)
            .await?;
    }
    storage::app::source_sink::update(&source_id, req).await?;

    Ok(())
}

pub async fn delete_source(app_id: String, source_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule::reference::count_cnt_by_resource_id(&source_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    storage::app::source_sink::delete_by_id(&source_id).await?;
    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        app.delete_source(source_id).await?;
    }

    Ok(())
}

pub async fn get_source_rxs(
    app_id: &String,
    source_id: &String,
    cnt: usize,
) -> HaliaResult<Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>> {
    if let Some(app) = GLOBAL_APP_MANAGER.get(app_id) {
        app.get_source_rxs(source_id, cnt).await
    } else {
        let name = storage::app::read_name(app_id).await?;
        Err(HaliaError::Stopped(name))
    }
}

pub async fn create_sink(app_id: String, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
    let typ: AppType = storage::app::read_type(&app_id).await?.try_into()?;
    match typ {
        AppType::MqttV311 => mqtt_v311::validate_sink_conf(&req.conf)?,
        AppType::MqttV50 => mqtt_v50::validate_sink_conf(&req.conf)?,
        AppType::Http => http::validate_sink_conf(&req.conf)?,
        AppType::Kafka => kafka::validate_sink_conf(&req.conf)?,
        AppType::InfluxdbV1 => influxdb_v1::validate_sink_conf(&req.conf)?,
        AppType::InfluxdbV2 => influxdb_v2::validate_sink_conf(&req.conf)?,
        AppType::Tdengine => tdengine::validate_sink_conf(&req.conf)?,
    }

    let sink_id = common::get_id();
    storage::app::source_sink::insert_sink(&app_id, &sink_id, req.clone()).await?;

    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let conf = req.conf.clone();
        app.create_sink(sink_id, conf).await?;
    }

    Ok(())
}

pub async fn search_sinks(
    app_id: String,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    let (count, db_sinks) =
        storage::app::source_sink::query_sinks_by_app_id(&app_id, pagination, query).await?;
    let mut data = Vec::with_capacity(db_sinks.len());
    for db_sink in db_sinks {
        let rule_ref = RuleRef {
            rule_ref_cnt: storage::rule::reference::count_cnt_by_resource_id(&db_sink.id).await?,
            rule_active_ref_cnt: storage::rule::reference::count_active_cnt_by_resource_id(
                &db_sink.id,
            )
            .await?,
        };
        data.push(SearchSourcesOrSinksItemResp {
            info: SearchSourcesOrSinksInfoResp {
                id: db_sink.id,
                conf: CreateUpdateSourceOrSinkReq {
                    name: db_sink.name,
                    conf: db_sink.conf,
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
    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        let old_conf = storage::app::source_sink::read_conf(&sink_id).await?;
        let new_conf = req.conf.clone();
        app.update_sink(sink_id.clone(), old_conf, new_conf).await?;
    }

    storage::app::source_sink::update(&sink_id, req.clone()).await?;

    Ok(())
}

pub async fn delete_sink(app_id: String, sink_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = storage::rule::reference::count_cnt_by_resource_id(&sink_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    storage::app::source_sink::delete_by_id(&sink_id).await?;

    if let Some(mut app) = GLOBAL_APP_MANAGER.get_mut(&app_id) {
        app.delete_sink(sink_id).await?;
    }

    Ok(())
}

pub async fn get_sink_txs(
    app_id: &String,
    sink_id: &String,
    cnt: usize,
) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>> {
    if let Some(app) = GLOBAL_APP_MANAGER.get(app_id) {
        app.get_sink_txs(sink_id, cnt).await
    } else {
        let name = storage::app::read_name(app_id).await?;
        Err(HaliaError::Stopped(name))
    }
}

async fn transer_db_app_to_resp(db_app: storage::app::App) -> HaliaResult<ListAppsItem> {
    let (can_stop, can_delete, err) = get_info_by_status(&db_app.id, &db_app.status).await?;
    let rule_reference_running_cnt =
        storage::rule::reference::count_running_cnt_by_parent_id(&db_app.id).await?;
    let rule_reference_total_cnt =
        storage::rule::reference::count_cnt_by_parent_id(&db_app.id).await?;
    let source_cnt = storage::app::source_sink::count_sources_by_app_id(&db_app.id).await?;
    let sink_cnt = storage::app::source_sink::count_sinks_by_app_id(&db_app.id).await?;

    Ok(ListAppsItem {
        id: db_app.id,
        name: db_app.name,
        typ: db_app.typ,
        status: db_app.status,
        err,
        rule_reference_running_cnt,
        rule_reference_total_cnt,
        source_cnt,
        sink_cnt,
        can_stop,
        can_delete,
    })
}

async fn get_info_by_status(
    app_id: &String,
    status: &Status,
) -> HaliaResult<(bool, bool, Option<String>)> {
    match status {
        types::Status::Running => {
            let can_stop =
                storage::rule::reference::count_active_cnt_by_parent_id(app_id).await? > 0;
            Ok((can_stop, false, None))
        }
        types::Status::Stopped => {
            let can_delete = storage::rule::reference::count_cnt_by_parent_id(app_id).await? > 0;
            Ok((false, can_delete, None))
        }
        types::Status::Error => {
            let can_stop =
                storage::rule::reference::count_active_cnt_by_parent_id(app_id).await? > 0;
            let app = match GLOBAL_APP_MANAGER.get(app_id) {
                Some(app) => app,
                None => return Err(HaliaError::Common("App未启动！".to_string())),
            };
            let err = app.read_err().await;
            Ok((can_stop, false, err))
        }
    }
}

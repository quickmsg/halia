use apps::App;
use common::{
    error::{HaliaError, HaliaResult},
    storage,
};
use dashmap::DashMap;
use databoard::databoard_struct::Databoard;
use devices::Device;
use rule::Rule;
use sqlx::AnyPool;
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};
use tokio::sync::RwLock;
use types::{
    rules::{CreateUpdateRuleReq, QueryParams, ReadRuleNodeResp, SearchRulesResp, Summary},
    Pagination,
};
use uuid::Uuid;

mod log;
pub mod rule;
mod segment;

static RULE_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static RULE_ON_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

fn get_rule_count() -> usize {
    RULE_COUNT.load(Ordering::SeqCst)
}

fn add_rule_count() {
    RULE_COUNT.fetch_add(1, Ordering::SeqCst);
}

fn sub_rule_count() {
    RULE_COUNT.fetch_sub(1, Ordering::SeqCst);
}

pub(crate) fn get_rule_on_count() -> usize {
    RULE_ON_COUNT.load(Ordering::SeqCst)
}

pub(crate) fn add_rule_on_count() {
    RULE_ON_COUNT.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn sub_rule_on_count() {
    RULE_ON_COUNT.fetch_sub(1, Ordering::SeqCst);
}

pub fn get_summary() -> Summary {
    Summary {
        total: get_rule_count(),
        on: get_rule_on_count(),
    }
}

pub async fn load_from_persistence(
    pool: &Arc<AnyPool>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
) -> HaliaResult<Arc<RwLock<Vec<Rule>>>> {
    let db_rules = storage::rule::read_rules(pool).await?;
    let rules: Arc<RwLock<Vec<Rule>>> = Arc::new(RwLock::new(vec![]));
    for db_rule in db_rules {
        let rule_id = Uuid::from_str(&db_rule.id).unwrap();
        create(
            pool,
            &rules,
            devices,
            apps,
            databoards,
            rule_id,
            db_rule.conf,
            false,
        )
        .await?;

        if db_rule.status == 1 {
            start(pool, &rules, &devices, &apps, &databoards, rule_id).await?;
        }
    }

    Ok(rules)
}

pub async fn create(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateRuleReq = serde_json::from_str(&body)?;
    let rule = Rule::new(devices, apps, databoards, id, req).await?;
    add_rule_count();
    if persist {
        storage::rule::create_rule(pool, &id, body).await?;
    }
    rules.write().await.push(rule);
    Ok(())
}

pub async fn search(
    rules: &Arc<RwLock<Vec<Rule>>>,
    pagination: Pagination,
    query_params: QueryParams,
) -> SearchRulesResp {
    let mut total = 0;
    let mut data = vec![];

    for rule in rules.read().await.iter().rev() {
        let rule = rule.search();
        if let Some(query_name) = &query_params.name {
            if !rule.conf.base.name.contains(query_name) {
                continue;
            }
        }

        if let Some(on) = &query_params.on {
            if rule.on != *on {
                continue;
            }
        }

        if pagination.check(total) {
            data.push(rule);
        }

        total += 1;
    }

    SearchRulesResp { total, data }
}

pub async fn read(
    rules: &Arc<RwLock<Vec<Rule>>>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    id: Uuid,
) -> HaliaResult<Vec<ReadRuleNodeResp>> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.read(devices, apps, databoards).await,
        None => return Err(HaliaError::NotFound),
    }
}

pub async fn start(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    id: Uuid,
) -> HaliaResult<()> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.start(devices, apps, databoards).await?,
        None => return Err(HaliaError::NotFound),
    }

    storage::rule::update_rule_status(pool, &id, true).await?;
    Ok(())
}

pub async fn stop(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    id: Uuid,
) -> HaliaResult<()> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.stop(devices, apps, databoards).await?,
        None => return Err(HaliaError::NotFound),
    }

    storage::rule::update_rule_status(pool, &id, false).await?;
    Ok(())
}

pub async fn update(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateRuleReq = serde_json::from_str(&body)?;

    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.update(devices, apps, databoards, req).await?,
        None => return Err(HaliaError::NotFound),
    }

    storage::rule::update_rule_conf(pool, &id, body).await?;

    Ok(())
}

pub async fn delete(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    devices: &Arc<DashMap<Uuid, Box<dyn Device>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    id: Uuid,
) -> HaliaResult<()> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.delete(devices, apps, databoards).await?,
        None => return Err(HaliaError::NotFound),
    }

    sub_rule_count();
    rules.write().await.retain(|rule| rule.id != id);
    storage::rule::delete_rule(pool, &id).await?;
    Ok(())
}

pub async fn get_log_filename(rules: &Arc<RwLock<Vec<Rule>>>, id: Uuid) -> HaliaResult<String> {
    let filename = match rules.read().await.iter().find(|rule| rule.id == id) {
        Some(rule) => rule.get_log_filename().await,
        None => return Err(HaliaError::NotFound),
    };

    Ok(filename)
}

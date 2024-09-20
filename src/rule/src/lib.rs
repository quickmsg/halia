use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, LazyLock,
};

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
use types::{
    rules::{CreateUpdateRuleReq, QueryParams, ReadRuleNodeResp, SearchRulesResp, Summary},
    Pagination,
};

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

// TODO
pub async fn load_from_storage(
    storage: &Arc<AnyPool>,
    devices: &Arc<DashMap<String, Box<dyn Device>>>,
    apps: &Arc<DashMap<String, Box<dyn App>>>,
    databoards: &Arc<DashMap<String, Databoard>>,
) -> HaliaResult<Arc<DashMap<String, Rule>>> {
    let db_rules = storage::rule::read_all(storage).await?;
    let rules: Arc<DashMap<String, Rule>> = Arc::new(DashMap::new());
    for db_rule in db_rules {
        if db_rule.status == 1 {
            start(storage, &rules, &devices, &apps, &databoards, db_rule.id).await?;
        }
    }

    Ok(rules)
}

pub async fn create(
    storage: &Arc<AnyPool>,
    rules: &Arc<DashMap<String, Rule>>,
    devices: &Arc<DashMap<String, Box<dyn Device>>>,
    apps: &Arc<DashMap<String, Box<dyn App>>>,
    databoards: &Arc<DashMap<String, Databoard>>,
    id: String,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateRuleReq = serde_json::from_str(&body)?;
    storage::rule::insert(storage, &id, body).await?;
    let rule = Rule::new(storage, id.clone(), req).await?;
    add_rule_count();
    rules.insert(id, rule);
    Ok(())
}

pub async fn search(
    rules: &Arc<DashMap<String, Rule>>,
    pagination: Pagination,
    query_params: QueryParams,
) -> SearchRulesResp {
    todo!()
    // let mut total = 0;
    // let mut data = vec![];

    // for rule in rules.read().await.iter().rev() {
    //     let rule = rule.search();
    //     if let Some(query_name) = &query_params.name {
    //         if !rule.conf.base.name.contains(query_name) {
    //             continue;
    //         }
    //     }

    //     if let Some(on) = &query_params.on {
    //         if rule.on != *on {
    //             continue;
    //         }
    //     }

    //     if pagination.check(total) {
    //         data.push(rule);
    //     }

    //     total += 1;
    // }

    // SearchRulesResp { total, data }
}

pub async fn read(
    storage: &Arc<AnyPool>,
    rules: &Arc<DashMap<String, Rule>>,
    devices: &Arc<DashMap<String, Box<dyn Device>>>,
    apps: &Arc<DashMap<String, Box<dyn App>>>,
    databoards: &Arc<DashMap<String, Databoard>>,
    id: String,
) -> HaliaResult<Vec<ReadRuleNodeResp>> {
    rules
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .read(storage, devices, apps, databoards)
        .await
}

pub async fn start(
    storage: &Arc<AnyPool>,
    rules: &Arc<DashMap<String, Rule>>,
    devices: &Arc<DashMap<String, Box<dyn Device>>>,
    apps: &Arc<DashMap<String, Box<dyn App>>>,
    databoards: &Arc<DashMap<String, Databoard>>,
    id: String,
) -> HaliaResult<()> {
    if rules.contains_key(&id) {
        return Ok(());
    }

    rules
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .start(storage, devices, apps, databoards)
        .await?;
    storage::rule::update_status(storage, &id, true).await?;
    Ok(())
}

pub async fn stop(
    storage: &Arc<AnyPool>,
    rules: &Arc<DashMap<String, Rule>>,
    devices: &Arc<DashMap<String, Box<dyn Device>>>,
    apps: &Arc<DashMap<String, Box<dyn App>>>,
    databoards: &Arc<DashMap<String, Databoard>>,
    id: String,
) -> HaliaResult<()> {
    if !rules.contains_key(&id) {
        return Ok(());
    }

    rules
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .stop(storage, databoards)
        .await?;

    storage::rule::update_status(storage, &id, false).await?;
    Ok(())
}

pub async fn update(
    storage: &Arc<AnyPool>,
    rules: &Arc<DashMap<String, Rule>>,
    devices: &Arc<DashMap<String, Box<dyn Device>>>,
    apps: &Arc<DashMap<String, Box<dyn App>>>,
    databoards: &Arc<DashMap<String, Databoard>>,
    id: String,
    req: CreateUpdateRuleReq,
) -> HaliaResult<()> {
    rules
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .update(storage, devices, apps, databoards, req)
        .await?;

    // storage::rule::update_rule_conf(pool, &id, req).await?;

    Ok(())
}

pub async fn delete(
    storage: &Arc<AnyPool>,
    rules: &Arc<DashMap<String, Rule>>,
    id: String,
) -> HaliaResult<()> {
    if rules.contains_key(&id) {
        return Err(HaliaError::DeleteRunning);
    }

    rules
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .delete(storage)
        .await?;

    rules.remove(&id);

    sub_rule_count();
    storage::rule::delete(storage, &id).await?;
    Ok(())
}

pub async fn get_log_filename(
    rules: &Arc<DashMap<String, Rule>>,
    id: String,
) -> HaliaResult<String> {
    let filename = rules
        .get(&id)
        .ok_or(HaliaError::NotFound)?
        .get_log_filename()
        .await;

    Ok(filename)
}

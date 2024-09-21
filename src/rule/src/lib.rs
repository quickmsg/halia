use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, LazyLock,
};

use common::{
    error::{HaliaError, HaliaResult},
    storage,
};
use dashmap::DashMap;
use rule::Rule;
use types::{
    rules::{CreateUpdateRuleReq, QueryParams, ReadRuleNodeResp, SearchRulesResp, Summary},
    Pagination,
};

mod log;
pub mod rule;
mod segment;

static GLOBAL_RULE_MANAGER: LazyLock<DashMap<String, Rule>> = LazyLock::new(|| DashMap::new());

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
pub async fn load_from_storage() -> HaliaResult<()> {
    let db_rules = storage::rule::read_all().await?;
    for db_rule in db_rules {
        if db_rule.status == 1 {
            start(db_rule.id).await?;
        }
    }

    Ok(())
}

pub async fn create(id: String, body: String) -> HaliaResult<()> {
    let req: CreateUpdateRuleReq = serde_json::from_str(&body)?;
    storage::rule::insert(&id, body).await?;
    let rule = Rule::new(id.clone(), req).await?;
    add_rule_count();
    GLOBAL_RULE_MANAGER.insert(id, rule);
    Ok(())
}

pub async fn search(pagination: Pagination, query_params: QueryParams) -> SearchRulesResp {
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

pub async fn read(id: String) -> HaliaResult<Vec<ReadRuleNodeResp>> {
    GLOBAL_RULE_MANAGER
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .read()
        .await
}

pub async fn start(id: String) -> HaliaResult<()> {
    if GLOBAL_RULE_MANAGER.contains_key(&id) {
        return Ok(());
    }

    GLOBAL_RULE_MANAGER
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .start()
        .await?;
    storage::rule::update_status(&id, true).await?;
    Ok(())
}

pub async fn stop(id: String) -> HaliaResult<()> {
    if !GLOBAL_RULE_MANAGER.contains_key(&id) {
        return Ok(());
    }

    GLOBAL_RULE_MANAGER
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .stop()
        .await?;

    storage::rule::update_status(&id, false).await?;
    Ok(())
}

pub async fn update(id: String, req: CreateUpdateRuleReq) -> HaliaResult<()> {
    GLOBAL_RULE_MANAGER
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .update(req)
        .await?;

    // storage::rule::update_rule_conf(pool, &id, req).await?;

    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    if GLOBAL_RULE_MANAGER.contains_key(&id) {
        return Err(HaliaError::DeleteRunning);
    }

    GLOBAL_RULE_MANAGER
        .get_mut(&id)
        .ok_or(HaliaError::NotFound)?
        .delete()
        .await?;

    GLOBAL_RULE_MANAGER.remove(&id);

    sub_rule_count();
    storage::rule::delete(&id).await?;
    Ok(())
}

pub async fn get_log_filename(id: String) -> HaliaResult<String> {
    let filename = GLOBAL_RULE_MANAGER
        .get(&id)
        .ok_or(HaliaError::NotFound)?
        .get_log_filename()
        .await;

    Ok(filename)
}

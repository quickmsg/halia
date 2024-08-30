use apps::App;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use devices::Device;
use rule::Rule;
use sqlx::AnyPool;
use std::{str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use types::{
    rules::{CreateUpdateRuleReq, QueryParams, SearchRulesResp, Summary},
    Pagination,
};
use uuid::Uuid;

pub mod rule;
mod segment;

pub async fn load_from_persistence(
    pool: &Arc<AnyPool>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
) -> HaliaResult<Arc<RwLock<Vec<Rule>>>> {
    let db_rules = persistence::rule::read_rules(pool).await?;
    let rules: Arc<RwLock<Vec<Rule>>> = Arc::new(RwLock::new(vec![]));
    for db_rule in db_rules {
        let rule_id = Uuid::from_str(&db_rule.id).unwrap();
        create(pool, &rules, devices, apps, rule_id, db_rule.conf, false).await?;

        if db_rule.status == 1 {
            start(pool, &rules, &devices, &apps, rule_id).await?;
        }
    }

    Ok(rules)
}

pub async fn get_summary(rules: &Arc<RwLock<Vec<Rule>>>) -> Summary {
    let mut total = 0;
    let mut running_cnt = 0;
    let mut off_cnt = 0;

    for rule in rules.read().await.iter() {
        let resp = rule.search();
        total += 1;
        if resp.on {
            running_cnt += 1;
        } else {
            off_cnt += 1;
        }
    }

    Summary {
        total,
        running_cnt,
        off_cnt,
    }
}

pub async fn create(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateRuleReq = serde_json::from_str(&body)?;
    let rule = Rule::new(devices, apps, id, req).await?;
    if persist {
        persistence::rule::create_rule(pool, &id, body).await?;
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

pub async fn start(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    id: Uuid,
) -> HaliaResult<()> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.start(devices, apps).await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::rule::update_rule_status(pool, &id, true).await?;
    Ok(())
}

pub async fn stop(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    id: Uuid,
) -> HaliaResult<()> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.stop()?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::rule::update_rule_status(pool, &id, false).await?;
    Ok(())
}

pub async fn update(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    id: Uuid,
    body: String,
) -> HaliaResult<()> {
    todo!()
}

pub async fn delete(
    pool: &Arc<AnyPool>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    id: Uuid,
) -> HaliaResult<()> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.stop()?,
        None => return Err(HaliaError::NotFound),
    }

    rules.write().await.retain(|rule| rule.id != id);
    persistence::rule::delete_rule(pool, &id).await?;
    Ok(())
}

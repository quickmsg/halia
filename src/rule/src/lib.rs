use apps::App;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{local::Local, Persistence},
};
use devices::Device;
use rule::Rule;
use std::{str::FromStr, sync::Arc};
use tokio::sync::{Mutex, RwLock};
use types::{
    rules::{CreateUpdateRuleReq, QueryParams, SearchRulesResp, Summary},
    Pagination,
};
use uuid::Uuid;

pub mod rule;
mod segment;

macro_rules! rule_not_fonnd_err {
    () => {
        Err(HaliaError::NotFound("规则".to_owned()))
    };
}

pub async fn load_from_persistence(
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
) -> HaliaResult<Arc<RwLock<Vec<Rule>>>> {
    let db_rules = persistence.lock().await.read_rules()?;
    let rules: Arc<RwLock<Vec<Rule>>> = Arc::new(RwLock::new(vec![]));
    for db_rule in db_rules {
        let rule_id = Uuid::from_str(&db_rule.id).unwrap();
        create(
            persistence,
            &rules,
            devices,
            apps,
            rule_id,
            db_rule.conf,
            false,
        )
        .await?;

        if db_rule.status == 1 {
            start(persistence, &rules, &devices, &apps, rule_id).await?;
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
    persistence: &Arc<Mutex<Local>>,
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
        persistence.lock().await.create_rule(&id, body)?;
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
    persistence: &Arc<Mutex<Local>>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    id: Uuid,
) -> HaliaResult<()> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.start(devices, apps).await?,
        None => return rule_not_fonnd_err!(),
    }

    persistence.lock().await.update_rule_status(&id, true)?;
    Ok(())
}

pub async fn stop(
    persistence: &Arc<Mutex<Local>>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    id: Uuid,
) -> HaliaResult<()> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.stop()?,
        None => return rule_not_fonnd_err!(),
    }

    persistence.lock().await.update_rule_status(&id, false)?;
    Ok(())
}

pub async fn update(
    persistence: &Arc<Mutex<Local>>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    id: Uuid,
    body: String,
) -> HaliaResult<()> {
    todo!()
}

pub async fn delete(
    persistence: &Arc<Mutex<Local>>,
    rules: &Arc<RwLock<Vec<Rule>>>,
    id: Uuid,
) -> HaliaResult<()> {
    match rules.write().await.iter_mut().find(|rule| rule.id == id) {
        Some(rule) => rule.stop()?,
        None => return rule_not_fonnd_err!(),
    }

    persistence.lock().await.delete_rule(&id)?;
    Ok(())
}

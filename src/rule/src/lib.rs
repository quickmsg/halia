use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};

use common::{
    error::{HaliaError, HaliaResult},
    storage,
};
use dashmap::DashMap;
use rule::Rule;
use types::{
    rules::{
        CreateUpdateRuleReq, QueryParams, ReadRuleNodeResp, RuleConf, SearchRulesItemResp,
        SearchRulesResp, Summary,
    },
    BaseConf, Pagination,
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

pub async fn load_from_storage() -> HaliaResult<()> {
    let db_on_rules = storage::rule::read_all_on().await?;
    for db_rule in db_on_rules {
        start(db_rule.id).await?;
    }

    Ok(())
}

pub async fn create(req: CreateUpdateRuleReq) -> HaliaResult<()> {
    if storage::rule::insert_name_exists(&req.base.name).await? {
        return Err(HaliaError::NameExists);
    }

    let id = common::get_id();
    Rule::db_new(&id, &req.ext).await?;
    storage::rule::insert(&id, req).await?;
    storage::event::insert(
        types::events::ResourceType::Rule,
        &id,
        types::events::EventType::Create,
        None,
    )
    .await?;
    add_rule_count();
    Ok(())
}

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> HaliaResult<SearchRulesResp> {
    let (count, db_rules) = storage::rule::query(pagination, query_params).await?;
    let rules = db_rules
        .into_iter()
        .map(|db_rule| SearchRulesItemResp {
            id: db_rule.id,
            on: db_rule.status == 1,
            conf: CreateUpdateRuleReq {
                base: BaseConf {
                    name: db_rule.name,
                    desc: db_rule
                        .des
                        .map(|desc| unsafe { String::from_utf8_unchecked(desc) }),
                },
                ext: serde_json::from_slice(&db_rule.conf).unwrap(),
            },
        })
        .collect::<Vec<_>>();

    Ok(SearchRulesResp {
        total: count,
        data: rules,
    })
}

pub async fn read(id: String) -> HaliaResult<Vec<ReadRuleNodeResp>> {
    let db_rule = storage::rule::read_one(&id).await?;
    let conf = serde_json::from_slice(&db_rule.conf)?;
    let detail = Rule::read(conf).await?;
    Ok(detail)
}

pub async fn start(id: String) -> HaliaResult<()> {
    if GLOBAL_RULE_MANAGER.contains_key(&id) {
        return Ok(());
    }

    add_rule_on_count();

    storage::event::insert(
        types::events::ResourceType::Rule,
        &id,
        types::events::EventType::Start,
        None,
    )
    .await?;

    let db_conf = storage::rule::read_conf(&id).await?;
    let conf: RuleConf = serde_json::from_slice(&db_conf)?;

    let rule = Rule::new(id.clone(), &conf).await?;
    GLOBAL_RULE_MANAGER.insert(id.clone(), rule);
    storage::rule::update_status(&id, true).await?;
    Ok(())
}

pub async fn stop(id: String) -> HaliaResult<()> {
    if !GLOBAL_RULE_MANAGER.contains_key(&id) {
        return Ok(());
    }

    match GLOBAL_RULE_MANAGER.remove(&id) {
        Some((_, mut rule)) => {
            rule.stop().await?;
            storage::rule::update_status(&id, false).await?;
            storage::event::insert(
                types::events::ResourceType::Rule,
                &id,
                types::events::EventType::Stop,
                None,
            )
            .await?;
            sub_rule_on_count();
        }
        None => return Err(HaliaError::NotFound(id)),
    }

    Ok(())
}

pub async fn update(id: String, req: CreateUpdateRuleReq) -> HaliaResult<()> {
    if storage::rule::update_name_exists(&id, &req.base.name).await? {
        return Err(HaliaError::NameExists);
    }

    if let Some(mut rule) = GLOBAL_RULE_MANAGER.get_mut(&id) {
        let old_conf: RuleConf = serde_json::from_slice(&storage::rule::read_conf(&id).await?)?;
        let new_conf = req.ext.clone();
        rule.update(old_conf, new_conf).await?;
    }

    storage::rule::update(&id, req).await?;
    storage::event::insert(
        types::events::ResourceType::Rule,
        &id,
        types::events::EventType::Update,
        None,
    )
    .await?;

    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    if GLOBAL_RULE_MANAGER.contains_key(&id) {
        return Err(HaliaError::DeleteRunning);
    }

    storage::event::insert(
        types::events::ResourceType::Rule,
        &id,
        types::events::EventType::Delete,
        None,
    )
    .await?;

    storage::rule::delete(&id).await?;
    storage::rule_ref::delete_many_by_rule_id(&id).await?;

    sub_rule_count();
    Ok(())
}

pub async fn get_log_filename(id: String) -> HaliaResult<String> {
    match GLOBAL_RULE_MANAGER.get(&id) {
        Some(rule) => {
            let filename = rule.get_log_filename().await;
            Ok(filename)
        }
        None => Err(HaliaError::NotFound(id)),
    }
}

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use rule::Rule;
use types::{
    rules::{
        AppSinkNode, AppSourceNode, Conf, CreateUpdateRuleReq, DataboardNode, DeviceSinkNode,
        DeviceSourceNode, Node, QueryParams, ReadRuleNodeResp, SearchRulesItemResp,
        SearchRulesResp, Summary,
    },
    Pagination,
};

mod graph;
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
    let id = common::get_id();
    create_rule_refs(&id, &req.conf.nodes).await?;

    storage::rule::insert(&id, req).await?;
    events::insert_create(types::events::ResourceType::Rule, &id).await;
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
        .map(|db_rule| {
            let log_enable = match GLOBAL_RULE_MANAGER.get(&db_rule.id) {
                Some(rule) => rule.get_log_status(),
                None => false,
            };
            SearchRulesItemResp {
                id: db_rule.id,
                on: db_rule.status == 1,
                conf: CreateUpdateRuleReq {
                    name: db_rule.name,
                    conf: serde_json::from_slice(&db_rule.conf).unwrap(),
                },
                log_enable,
            }
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
    events::insert_update(types::events::ResourceType::Rule, &id).await;
    let db_conf = storage::rule::read_conf(&id).await?;
    let conf: Conf = serde_json::from_slice(&db_conf)?;

    let rule = Rule::new(id.clone(), &conf).await?;
    GLOBAL_RULE_MANAGER.insert(id.clone(), rule);
    storage::rule::reference::active(&id).await?;
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
            events::insert_stop(types::events::ResourceType::Rule, &id).await;
            sub_rule_on_count();
        }
        None => return Err(HaliaError::NotFound(id)),
    }

    Ok(())
}

pub async fn update(id: String, req: CreateUpdateRuleReq) -> HaliaResult<()> {
    storage::rule::reference::delete_many_by_rule_id(&id).await?;
    create_rule_refs(&id, &req.conf.nodes).await?;

    if let Some(mut rule) = GLOBAL_RULE_MANAGER.get_mut(&id) {
        let old_conf: Conf = serde_json::from_slice(&storage::rule::read_conf(&id).await?)?;
        let new_conf = req.conf.clone();
        if old_conf != new_conf {
            rule.update(old_conf, new_conf).await?;
        }
    }

    storage::rule::update(&id, req).await?;
    events::insert_update(types::events::ResourceType::Rule, &id).await;
    Ok(())
}

pub async fn delete(id: String) -> HaliaResult<()> {
    if GLOBAL_RULE_MANAGER.contains_key(&id) {
        return Err(HaliaError::DeleteRunning);
    }

    events::insert_delete(types::events::ResourceType::Rule, &id).await;
    storage::rule::delete_by_id(&id).await?;
    storage::rule::reference::delete_many_by_rule_id(&id).await?;

    sub_rule_count();
    Ok(())
}

async fn create_rule_refs(id: &String, nodes: &Vec<Node>) -> HaliaResult<()> {
    let mut err = None;
    for node in nodes {
        match node.node_type {
            types::rules::NodeType::DeviceSource => {
                let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                if !storage::device::source_sink::check_exists(&source_node.source_id).await? {
                    err = Some(format!("设备源 {} 不存在！", source_node.source_id));
                    break;
                }
                storage::rule::reference::insert(
                    &id,
                    &source_node.device_id,
                    &source_node.source_id,
                )
                .await?;
            }
            types::rules::NodeType::AppSource => {
                let source_node: AppSourceNode = serde_json::from_value(node.conf.clone())?;
                if !storage::source_or_sink::check_exists(&source_node.source_id).await? {
                    err = Some(format!("应用源 {} 不存在！", source_node.source_id));
                    break;
                }
                storage::rule::reference::insert(&id, &source_node.app_id, &source_node.source_id)
                    .await?;
            }
            types::rules::NodeType::DeviceSink => {
                let sink_node: DeviceSinkNode = serde_json::from_value(node.conf.clone())?;
                if !storage::device::source_sink::check_exists(&sink_node.sink_id).await? {
                    err = Some(format!("设备动作 {} 不存在！", sink_node.sink_id));
                    break;
                }
                storage::rule::reference::insert(&id, &sink_node.device_id, &sink_node.sink_id)
                    .await?;
            }
            types::rules::NodeType::AppSink => {
                let sink_node: AppSinkNode = serde_json::from_value(node.conf.clone())?;
                if !storage::source_or_sink::check_exists(&sink_node.sink_id).await? {
                    err = Some(format!("应用动作 {} 不存在！", sink_node.sink_id));
                    break;
                }
                storage::rule::reference::insert(&id, &sink_node.app_id, &sink_node.sink_id)
                    .await?;
            }
            types::rules::NodeType::Databoard => {
                let databoard_node: DataboardNode = serde_json::from_value(node.conf.clone())?;
                if !storage::databoard::data::check_exists(&databoard_node.data_id).await? {
                    err = Some(format!("数据看板数据 {} 不存在！", databoard_node.data_id));
                    break;
                }
                storage::rule::reference::insert(
                    &id,
                    &databoard_node.databoard_id,
                    &databoard_node.data_id,
                )
                .await?;
            }
            _ => {}
        }
    }

    match err {
        Some(e) => {
            storage::rule::reference::delete_many_by_rule_id(&id).await?;
            return Err(HaliaError::NotFound(e));
        }
        None => Ok(()),
    }
}

pub async fn start_log(id: String) -> HaliaResult<()> {
    match GLOBAL_RULE_MANAGER.get_mut(&id) {
        Some(mut rule) => {
            rule.start_log().await;
            Ok(())
        }
        None => Err(HaliaError::NotFound(id)),
    }
}

pub async fn stop_log(id: String) -> HaliaResult<()> {
    match GLOBAL_RULE_MANAGER.get_mut(&id) {
        Some(mut rule) => {
            rule.stop_log().await;
            Ok(())
        }
        None => Err(HaliaError::NotFound(id)),
    }
}
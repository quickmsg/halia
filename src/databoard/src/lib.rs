use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use databoard_struct::Databoard;
use message::RuleMessageBatch;
use tokio::sync::mpsc;
use types::{
    databoard::{
        CreateUpdateDataReq, CreateUpdateDataboardReq, ListDataboardsItem, ListDataboardsResp,
        ListDatasItemResp, ListDatasResp, QueryDatasParams, QueryParams, QueryRuleInfo,
        RuleInfoData, RuleInfoDataboard, RuleInfoResp, Summary,
    },
    Pagination,
};

pub mod data;
pub mod databoard_struct;

static GLOBAL_DATABOARD_MANAGER: LazyLock<DashMap<String, Databoard>> =
    LazyLock::new(|| DashMap::new());

static DATABOARD_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));
static DATABOARD_ON_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

fn get_databoard_count() -> usize {
    DATABOARD_COUNT.load(Ordering::SeqCst)
}

fn add_databoard_count() {
    DATABOARD_COUNT.fetch_add(1, Ordering::SeqCst);
}

fn sub_databoard_count() {
    DATABOARD_COUNT.fetch_sub(1, Ordering::SeqCst);
}

fn get_databoard_on_count() -> usize {
    DATABOARD_ON_COUNT.load(Ordering::SeqCst)
}

fn add_databoard_on_count() {
    DATABOARD_ON_COUNT.fetch_add(1, Ordering::SeqCst);
}

fn sub_databoard_on_count() {
    DATABOARD_ON_COUNT.fetch_sub(1, Ordering::SeqCst);
}

pub fn get_summary() -> Summary {
    Summary {
        total: get_databoard_count(),
        on: get_databoard_on_count(),
    }
}

pub async fn load_from_storage() -> HaliaResult<()> {
    let count = storage::databoard::count().await?;
    DATABOARD_COUNT.store(count, Ordering::SeqCst);

    let db_on_databoards = storage::databoard::read_all_running().await?;
    let on_count = db_on_databoards.len();
    DATABOARD_ON_COUNT.store(on_count, Ordering::SeqCst);

    for db_on_databoard in db_on_databoards {
        let mut databoard = Databoard::new();

        let db_datas =
            storage::databoard::data::read_all_by_databoard_id(&db_on_databoard.id).await?;
        for db_data in db_datas {
            databoard.create_data(db_data.id, db_data.conf).await?;
        }

        GLOBAL_DATABOARD_MANAGER.insert(db_on_databoard.id, databoard);
    }

    Ok(())
}

pub async fn get_rule_info(query: QueryRuleInfo) -> HaliaResult<RuleInfoResp> {
    let db_databoard = storage::databoard::read_one(&query.databoard_id).await?;
    let db_databoard_data = storage::databoard::data::read_one(&query.data_id).await?;

    Ok(RuleInfoResp {
        databoard: RuleInfoDataboard {
            id: db_databoard.id,
            name: db_databoard.name,
            status: db_databoard.status,
        },
        data: RuleInfoData {
            id: db_databoard_data.id,
            name: db_databoard_data.name,
        },
    })
}

pub async fn list_databoards(
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<ListDataboardsResp> {
    let (count, db_databoards) = storage::databoard::query(pagination, query).await?;

    let mut list = Vec::with_capacity(db_databoards.len());
    for db_databoard in db_databoards {
        let data_count = storage::databoard::data::count_by_databoard_id(&db_databoard.id).await?;
        let rule_ref_cnt =
            storage::rule::reference::get_rule_ref_info_by_parent_id(&db_databoard.id).await?;
        let (can_stop, can_delete) = match &db_databoard.status {
            types::Status::Running => {
                let can_stop = rule_ref_cnt.rule_reference_running_cnt == 0;
                (can_stop, false)
            }
            types::Status::Stopped => {
                let can_delete = rule_ref_cnt.rule_reference_total_cnt == 0;
                (true, can_delete)
            }
            _ => unreachable!("数据看板不可能出现错误情况。"),
        };
        list.push(ListDataboardsItem {
            id: db_databoard.id,
            name: db_databoard.name,
            status: db_databoard.status,
            data_count,
            rule_ref_cnt,
            can_stop,
            can_delete,
        });
    }

    Ok(ListDataboardsResp { count, list })
}

pub async fn create_databoard(req: CreateUpdateDataboardReq) -> HaliaResult<()> {
    let id = common::get_id();
    add_databoard_count();
    storage::databoard::insert(&id, req).await?;
    Ok(())
}

pub async fn update_databoard(
    databoard_id: String,
    req: CreateUpdateDataboardReq,
) -> HaliaResult<()> {
    storage::databoard::update_conf(&databoard_id, req).await?;
    Ok(())
}

pub async fn start_databoard(databoard_id: String) -> HaliaResult<()> {
    if GLOBAL_DATABOARD_MANAGER.contains_key(&databoard_id) {
        return Ok(());
    }

    // let db_databoard = storage::databoard::read_one(&databoard_id).await?;
    let databoard = Databoard::new();
    GLOBAL_DATABOARD_MANAGER.insert(databoard_id.clone(), databoard);

    let mut databoard = GLOBAL_DATABOARD_MANAGER.get_mut(&databoard_id).unwrap();
    let db_datas = storage::databoard::data::read_all_by_databoard_id(&databoard_id).await?;
    for db_data in db_datas {
        databoard.create_data(db_data.id, db_data.conf).await?;
    }

    add_databoard_on_count();

    storage::databoard::update_status(&databoard_id, types::Status::Running).await?;

    Ok(())
}

pub async fn stop_databoard(databoard_id: String) -> HaliaResult<()> {
    if !GLOBAL_DATABOARD_MANAGER.contains_key(&databoard_id) {
        return Ok(());
    }
    if storage::rule::reference::count_cnt_by_parent_id(&databoard_id, Some(types::Status::Running))
        .await?
        > 0
    {
        return Err(HaliaError::StopActiveRefing);
    }

    match GLOBAL_DATABOARD_MANAGER.remove(&databoard_id) {
        Some((_, mut databoard)) => {
            databoard.stop().await;
            sub_databoard_on_count();
            storage::databoard::update_status(&databoard_id, types::Status::Stopped).await?;
            Ok(())
        }
        None => Err(HaliaError::NotFound(databoard_id)),
    }
}

pub async fn delete_databoard(databoard_id: String) -> HaliaResult<()> {
    if GLOBAL_DATABOARD_MANAGER.contains_key(&databoard_id) {
        return Err(HaliaError::DeleteRunning);
    }

    if storage::rule::reference::count_cnt_by_parent_id(&databoard_id, None).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    sub_databoard_count();
    storage::databoard::delete_by_id(&databoard_id).await?;

    Ok(())
}

pub async fn create_data(databoard_id: String, req: CreateUpdateDataReq) -> HaliaResult<()> {
    let data_id = common::get_id();

    if let Some(mut databoard) = GLOBAL_DATABOARD_MANAGER.get_mut(&databoard_id) {
        let conf = req.conf.clone();
        databoard.create_data(data_id.clone(), conf).await?;
    }

    storage::databoard::data::insert(&data_id, &databoard_id, req).await?;

    Ok(())
}

pub async fn list_datas(
    databoard_id: String,
    pagination: Pagination,
    query: QueryDatasParams,
) -> HaliaResult<ListDatasResp> {
    let (count, db_datas) =
        storage::databoard::data::search(&databoard_id, pagination, query).await?;

    let mut list = Vec::with_capacity(db_datas.len());

    let databoard = GLOBAL_DATABOARD_MANAGER.get(&databoard_id);

    for db_data in db_datas {
        let (value, ts) = match &databoard {
            Some(databoard) => {
                let runtime = databoard.read_data_runtime(&db_data.id).await?;
                (Some(runtime.value), Some(runtime.ts))
            }
            None => (None, None),
        };
        let rule_ref_cnt =
            storage::rule::reference::get_rule_ref_info_by_parent_id(&db_data.id).await?;
        let can_delete = rule_ref_cnt.rule_reference_total_cnt == 0;
        list.push(ListDatasItemResp {
            id: db_data.id,
            name: db_data.name,
            conf: db_data.conf,
            value,
            ts,
            rule_ref_cnt,
            can_delete,
        });
    }

    Ok(ListDatasResp { count, list })
}

pub async fn update_data(
    _databoard_id: String,
    databoard_data_id: String,
    req: CreateUpdateDataReq,
) -> HaliaResult<()> {
    storage::databoard::data::update(&databoard_data_id, req).await?;

    Ok(())
}

pub async fn delete_data(databoard_id: String, databoard_data_id: String) -> HaliaResult<()> {
    if storage::rule::reference::count_cnt_by_resource_id(&databoard_data_id, None).await? > 0 {
        return Err(HaliaError::DeleteRefing);
    }
    if let Some(databoard) = GLOBAL_DATABOARD_MANAGER.get_mut(&databoard_id) {
        databoard.delete_data(&databoard_data_id).await?;
    }
    storage::databoard::data::delete_by_id(&databoard_data_id).await?;

    Ok(())
}

pub async fn get_data_txs(
    databoard_id: &String,
    databoard_data_id: &String,
    cnt: usize,
) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>> {
    match GLOBAL_DATABOARD_MANAGER.get(databoard_id) {
        Some(databoard) => databoard.get_data_txs(databoard_data_id, cnt).await,
        None => {
            let name = storage::databoard::read_name(databoard_id).await?;
            Err(HaliaError::Stopped(format!("看板：{}", name)))
        }
    }
}

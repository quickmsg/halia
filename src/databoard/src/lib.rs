use std::sync::{
    atomic::{AtomicUsize, Ordering},
    LazyLock,
};

use common::{
    error::{HaliaError, HaliaResult},
    storage::{self, rule_ref},
};
use dashmap::DashMap;
use databoard_struct::Databoard;
use message::MessageBatch;
use tokio::sync::mpsc;
use types::{
    databoard::{
        CreateUpdateDataReq, CreateUpdateDataboardReq, DataConf, DataboardConf, QueryParams,
        QueryRuleInfo, SearchDataboardsItemResp, SearchDataboardsResp, SearchDatasResp,
        SearchRuleInfo, Summary,
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

    let db_on_databoards = storage::databoard::read_many_on().await?;
    let on_count = db_on_databoards.len();
    DATABOARD_ON_COUNT.store(on_count, Ordering::SeqCst);

    for db_on_databoard in db_on_databoards {
        let conf: DataboardConf = serde_json::from_str(&db_on_databoard.conf)?;
        let mut databoard = Databoard::new(conf);

        let db_datas = storage::databoard_data::read_many(&db_on_databoard.id).await?;
        for db_data in db_datas {
            let data_conf: DataConf = serde_json::from_str(&db_data.conf)?;
            databoard.create_data(db_data.id, data_conf).await?;
        }

        GLOBAL_DATABOARD_MANAGER.insert(db_on_databoard.id, databoard);
    }

    Ok(())
}

pub async fn get_rule_info(query: QueryRuleInfo) -> HaliaResult<SearchRuleInfo> {
    todo!()
    // match databoards
    //     .read()
    //     .await
    //     .iter()
    //     .find(|databoard| databoard.id == query.databoard_id)
    // {
    //     Some(databoard) => {
    //         let databoard_info = databoard.search();
    //         let data_info = databoard.search_data(&query.data_id).await?;
    //         Ok(SearchRuleInfo {
    //             databoard: databoard_info,
    //             data: data_info,
    //         })
    //     }
    //     None => Err(HaliaError::NotFound),
    // }
}

pub async fn create_databoard(req: CreateUpdateDataboardReq) -> HaliaResult<()> {
    let id = common::get_id();
    add_databoard_count();
    storage::databoard::insert(&id, req).await?;
    Ok(())
}

pub async fn search_databoards(
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchDataboardsResp> {
    let (count, db_databoards) = storage::databoard::query(pagination, query).await?;

    let mut resp_databoards = vec![];
    for db_databoard in db_databoards {
        resp_databoards.push(SearchDataboardsItemResp {
            id: db_databoard.id,
            conf: CreateUpdateDataboardReq {
                base: serde_json::from_str(&db_databoard.conf).unwrap(),
                ext: DataboardConf {},
            },
        });
    }

    Ok(SearchDataboardsResp {
        total: count,
        data: resp_databoards,
    })
}

pub async fn update_databoard(
    databoard_id: String,
    req: CreateUpdateDataboardReq,
) -> HaliaResult<()> {
    // if let Some(mut databoard) = databoards.get_mut(&databoard_id) {
    //     databoard.update(req).await?;
    // }

    storage::databoard::update_conf(&databoard_id, req).await?;

    Ok(())
}

pub async fn start_databoard(databoard_id: String) -> HaliaResult<()> {
    if GLOBAL_DATABOARD_MANAGER.contains_key(&databoard_id) {
        return Ok(());
    }

    let db_databoard = storage::databoard::read_one(&databoard_id).await?;

    let databoard_conf: DataboardConf = serde_json::from_str(&db_databoard.conf)?;
    let databoard = Databoard::new(databoard_conf);
    GLOBAL_DATABOARD_MANAGER.insert(databoard_id.clone(), databoard);

    let mut databoard = GLOBAL_DATABOARD_MANAGER.get_mut(&databoard_id).unwrap();
    let db_datas = storage::databoard_data::read_many(&databoard_id).await?;
    for db_data in db_datas {
        let data_conf: DataConf = serde_json::from_str(&db_data.conf)?;
        databoard.create_data(db_data.id, data_conf).await?;
    }

    add_databoard_on_count();

    storage::databoard::update_status(&databoard_id, true).await?;

    Ok(())
}

pub async fn stop_databoard() {
    todo!()
}

pub async fn delete_databoard(databoard_id: String) -> HaliaResult<()> {
    let rule_ref_cnt = rule_ref::count_cnt_by_parent_id(&databoard_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    GLOBAL_DATABOARD_MANAGER
        .get_mut(&databoard_id)
        .ok_or(HaliaError::NotFound)?
        .stop()
        .await;

    sub_databoard_count();
    GLOBAL_DATABOARD_MANAGER.remove(&databoard_id);
    storage::databoard::delete(&databoard_id).await?;

    Ok(())
}

pub async fn create_data(
    databoard_id: String,
    req: CreateUpdateDataReq,
    persist: bool,
) -> HaliaResult<()> {
    let data_id = common::get_id();

    if let Some(mut databoard) = GLOBAL_DATABOARD_MANAGER.get_mut(&databoard_id) {
        let conf = req.ext.clone();
        databoard.create_data(data_id.clone(), conf).await?;
    }

    storage::databoard_data::insert(&databoard_id, &data_id, req).await?;

    Ok(())
}

pub async fn search_datas(
    databoard_id: String,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchDatasResp> {
    // match databoards
    //     .read()
    //     .await
    //     .iter()
    //     .find(|databoard| databoard.id == databoard_id)
    // {
    //     Some(device) => Ok(device.search_datas(pagination, query).await),
    //     None => Err(HaliaError::NotFound),
    // }
    todo!()
}

pub async fn update_data(
    databoard_id: String,
    databoard_data_id: String,
    req: CreateUpdateDataReq,
) -> HaliaResult<()> {
    // match databoards
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|databoard| databoard.id == databoard_id)
    // {
    //     Some(databoard) => databoard.update_data(databoard_data_id, req).await?,
    //     None => return Err(HaliaError::NotFound),
    // }

    storage::databoard_data::update(&databoard_data_id, req).await?;

    Ok(())
}

pub async fn delete_data(databoard_id: String, databoard_data_id: String) -> HaliaResult<()> {
    // match databoards
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|databoard| databoard.id == databoard_id)
    // {
    //     Some(databoard) => databoard.delete_data(databoard_data_id).await?,
    //     None => return Err(HaliaError::NotFound),
    // }

    storage::databoard_data::delete_one(&databoard_data_id).await?;

    Ok(())
}

pub async fn get_data_tx(
    databoard_id: &String,
    databoard_data_id: &String,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    GLOBAL_DATABOARD_MANAGER
        .get_mut(databoard_id)
        .ok_or(HaliaError::NotFound)?
        .get_data_tx(databoard_data_id)
        .await
}

use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};

use common::{
    error::{HaliaError, HaliaResult},
    storage::{self, rule_ref},
};
use dashmap::DashMap;
use databoard_struct::Databoard;
use message::MessageBatch;
use sqlx::AnyPool;
use tokio::sync::mpsc;
use types::{
    databoard::{
        CreateUpdateDataReq, CreateUpdateDataboardReq, DataConf, DataboardConf, QueryParams,
        QueryRuleInfo, SearchDataboardsItemResp, SearchDataboardsResp, SearchDatasResp,
        SearchRuleInfo, Summary,
    },
    Pagination,
};
use uuid::Uuid;

pub mod data;
pub mod databoard_struct;

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

pub async fn load_from_storage(
    storage: &Arc<AnyPool>,
) -> HaliaResult<Arc<DashMap<Uuid, Databoard>>> {
    let databoards: Arc<DashMap<Uuid, Databoard>> = Arc::new(DashMap::new());

    let count = storage::databoard::count(storage).await?;
    DATABOARD_COUNT.store(count, Ordering::SeqCst);

    let db_on_databoards = storage::databoard::read_many_on(storage).await?;
    let on_count = db_on_databoards.len();
    DATABOARD_ON_COUNT.store(on_count, Ordering::SeqCst);

    for db_on_databoard in db_on_databoards {
        let conf: DataboardConf = serde_json::from_str(&db_on_databoard.conf)?;
        let mut databoard = Databoard::new(conf);

        let databoard_id = Uuid::from_str(&db_on_databoard.id).unwrap();
        let db_datas = storage::databoard_data::read_many(storage, &databoard_id).await?;
        for db_data in db_datas {
            let data_conf: DataConf = serde_json::from_str(&db_data.conf)?;
            databoard
                .create_data(Uuid::from_str(&db_data.id).unwrap(), data_conf)
                .await?;
        }

        databoards.insert(databoard_id, databoard);
    }

    Ok(databoards)
}

pub async fn get_rule_info(
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    query: QueryRuleInfo,
) -> HaliaResult<SearchRuleInfo> {
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

pub async fn create_databoard(
    storage: &Arc<AnyPool>,
    req: CreateUpdateDataboardReq,
) -> HaliaResult<()> {
    let id = Uuid::new_v4();
    add_databoard_count();
    storage::databoard::insert(storage, &id, req).await?;
    Ok(())
}

pub async fn search_databoards(
    storage: &Arc<AnyPool>,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchDataboardsResp> {
    let (count, db_databoards) = storage::databoard::query(storage, pagination, query).await?;

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
    storage: &Arc<AnyPool>,
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
    req: CreateUpdateDataboardReq,
) -> HaliaResult<()> {
    // if let Some(mut databoard) = databoards.get_mut(&databoard_id) {
    //     databoard.update(req).await?;
    // }

    storage::databoard::update_conf(storage, &databoard_id, req).await?;

    Ok(())
}

pub async fn start_databoard(
    storage: &Arc<AnyPool>,
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
) -> HaliaResult<()> {
    if databoards.contains_key(&databoard_id) {
        return Ok(());
    }

    let db_databoard = storage::databoard::read_one(storage, &databoard_id).await?;

    let databoard_conf: DataboardConf = serde_json::from_str(&db_databoard.conf)?;
    let databoard = Databoard::new(databoard_conf);
    databoards.insert(databoard_id, databoard);

    let mut databoard = databoards.get_mut(&databoard_id).unwrap();
    let db_datas = storage::databoard_data::read_many(storage, &databoard_id).await?;
    for db_data in db_datas {
        let data_conf: DataConf = serde_json::from_str(&db_data.conf)?;
        databoard
            .create_data(Uuid::from_str(&db_data.id).unwrap(), data_conf)
            .await?;
    }

    add_databoard_on_count();

    storage::databoard::update_status(storage, &databoard_id, true).await?;

    Ok(())
}

pub async fn stop_databoard() {
    todo!()
}

pub async fn delete_databoard(
    storage: &Arc<AnyPool>,
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
) -> HaliaResult<()> {
    let rule_ref_cnt = rule_ref::count_cnt_by_parent_id(storage, &databoard_id).await?;
    if rule_ref_cnt > 0 {
        return Err(HaliaError::DeleteRefing);
    }

    databoards
        .get_mut(&databoard_id)
        .ok_or(HaliaError::NotFound)?
        .stop()
        .await;

    sub_databoard_count();
    databoards.remove(&databoard_id);
    storage::databoard::delete(storage, &databoard_id).await?;

    Ok(())
}

pub async fn create_data(
    storage: &Arc<AnyPool>,
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
    req: CreateUpdateDataReq,
    persist: bool,
) -> HaliaResult<()> {
    let data_id = Uuid::new_v4();
    if let Some(mut databoard) = databoards.get_mut(&databoard_id) {
        let conf = req.ext.clone();
        databoard.create_data(data_id, conf).await?;
    }

    storage::databoard_data::insert(storage, &databoard_id, &data_id, req).await?;

    Ok(())
}

pub async fn search_datas(
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
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
    storage: &Arc<AnyPool>,
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
    databoard_data_id: Uuid,
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

    storage::databoard_data::update(storage, &databoard_data_id, req).await?;

    Ok(())
}

pub async fn delete_data(
    storage: &Arc<AnyPool>,
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
    databoard_data_id: Uuid,
) -> HaliaResult<()> {
    // match databoards
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|databoard| databoard.id == databoard_id)
    // {
    //     Some(databoard) => databoard.delete_data(databoard_data_id).await?,
    //     None => return Err(HaliaError::NotFound),
    // }

    storage::databoard_data::delete_one(storage, &databoard_data_id).await?;

    Ok(())
}

pub async fn get_data_tx(
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    databoards
        .get_mut(databoard_id)
        .ok_or(HaliaError::NotFound)?
        .get_data_tx(databoard_data_id)
        .await
}

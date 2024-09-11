use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};

use common::{
    error::{HaliaError, HaliaResult},
    storage,
};
use dashmap::DashMap;
use databoard_struct::Databoard;
use message::MessageBatch;
use sqlx::AnyPool;
use tokio::sync::mpsc;
use tracing::debug;
use types::{
    databoard::{
        CreateUpdateDataReq, CreateUpdateDataboardReq, QueryParams, QueryRuleInfo,
        SearchDataboardsResp, SearchDatasResp, SearchRuleInfo, Summary,
    },
    Pagination,
};
use uuid::Uuid;

pub mod data;
pub mod databoard_struct;

static DATABOARD_COUNT: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

fn get_databoard_count() -> usize {
    DATABOARD_COUNT.load(Ordering::SeqCst)
}

fn add_databoard_count() {
    DATABOARD_COUNT.fetch_add(1, Ordering::SeqCst);
}

fn sub_databoard_count() {
    DATABOARD_COUNT.fetch_sub(1, Ordering::SeqCst);
}

pub fn get_summary() -> Summary {
    Summary {
        total: get_databoard_count(),
    }
}

pub async fn load_from_storage(
    storage: &Arc<AnyPool>,
) -> HaliaResult<Arc<DashMap<Uuid, Databoard>>> {
    let db_databoards = storage::databoard::read_databoards(storage).await?;
    let databoards: Arc<DashMap<Uuid, Databoard>> = Arc::new(DashMap::new());
    for db_databoard in db_databoards {
        let databoard_id = Uuid::from_str(&db_databoard.id).unwrap();

        let db_datas = storage::databoard::read_databoard_datas(storage, &databoard_id).await?;
        debug!("{}", db_datas.len());
        // create_databoard(storage, &databoards, databoard_id, db_databoard.conf).await?;

        // for db_data in db_datas {
        //     create_data(
        //         storage,
        //         &databoards,
        //         databoard_id,
        //         Uuid::from_str(&db_data.id).unwrap(),
        //         db_data.conf,
        //         false,
        //     )
        //     .await?;
        // }
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
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    req: CreateUpdateDataboardReq,
) -> HaliaResult<()> {
    // let databoard = Databoard::new(id, req.base, req.ext)?;
    // databoards.write().await.push(databoard);
    add_databoard_count();
    let id = Uuid::new_v4();
    storage::databoard::create_databoard(storage, &id, req).await?;
    Ok(())
}

pub async fn search_databoards(
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    pagination: Pagination,
    query: QueryParams,
) -> SearchDataboardsResp {
    todo!()
    // let mut data = vec![];
    // let mut total = 0;

    // for databoard in databoards.read().await.iter().rev() {
    //     let databoard = databoard.search();
    //     if let Some(name) = &query.name {
    //         if !databoard.conf.base.name.contains(name) {
    //             continue;
    //         }
    //     }

    //     if pagination.check(total) {
    //         data.push(databoard);
    //     }

    //     total += 1;
    // }

    // SearchDataboardsResp { total, data }
}

pub async fn update_databoard(
    storage: &Arc<AnyPool>,
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
    req: CreateUpdateDataboardReq,
) -> HaliaResult<()> {
    // match databoards
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|databoard| databoard.id == databoard_id)
    // {
    //     Some(databoard) => databoard.update(req.base)?,
    //     None => return Err(HaliaError::NotFound),
    // }

    storage::databoard::update_databoard(storage, &databoard_id, req).await?;

    Ok(())
}

pub async fn delete_databoard(
    storage: &Arc<AnyPool>,
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
) -> HaliaResult<()> {
    // 运行中，不能被直接删除
    if let Some(_) = databoards.get(&databoard_id) {
        return Err(HaliaError::DeleteRunning);
    }
    // 判断子资源引用情况
    // 判断是否停止中
    // match databoards
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|databoard| databoard.id == databoard_id)
    // {
    //     Some(databoard) => databoard.delete()?,
    //     None => return Err(HaliaError::NotFound),
    // }

    // databoards
    //     .write()
    //     .await
    //     .retain(|databoard| databoard.id != databoard_id);
    sub_databoard_count();
    storage::databoard::delete_databoard(storage, &databoard_id).await?;

    Ok(())
}

pub async fn create_data(
    storage: &Arc<AnyPool>,
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: Uuid,
    databoard_data_id: Uuid,
    req: CreateUpdateDataReq,
    persist: bool,
) -> HaliaResult<()> {
    databoards
        .get_mut(&databoard_id)
        .ok_or(HaliaError::NotFound)?
        .create_data(databoard_data_id, req)
        .await?;

    // if persist {
    //     storage::databoard::create_databoard_data(storage, &databoard_id, &databoard_data_id, req)
    //         .await?;
    // }

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

    storage::databoard::update_databoard_data(storage, &databoard_data_id, req).await?;

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

    storage::databoard::delete_databoard_data(storage, &databoard_data_id).await?;

    Ok(())
}

pub async fn add_data_ref(
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    // match databoards
    //     .write()
    //     .await
    //     .iter_mut()
    //     .find(|databoard| databoard.id == *databoard_id)
    // {
    //     Some(databoard) => databoard.add_data_ref(&databoard_data_id, &rule_id).await,
    //     None => return Err(HaliaError::NotFound),
    // }
    todo!()
}

pub async fn get_data_tx(
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    databoards
        .get_mut(databoard_id)
        .ok_or(HaliaError::NotFound)?
        .get_data_tx(databoard_data_id, rule_id)
        .await
}

pub async fn del_data_tx(
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    databoards
        .get_mut(databoard_id)
        .ok_or(HaliaError::NotFound)?
        .del_data_tx(databoard_data_id, rule_id)
        .await
}

pub async fn del_data_ref(
    databoards: &Arc<DashMap<Uuid, Databoard>>,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    databoards
        .get_mut(databoard_id)
        .ok_or(HaliaError::NotFound)?
        .del_data_ref(databoard_data_id, rule_id)
        .await
}

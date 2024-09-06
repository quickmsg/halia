use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};

use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use databoard_struct::Databoard;
use message::MessageBatch;
use sqlx::AnyPool;
use tokio::sync::{mpsc, RwLock};
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

pub async fn load_from_persistence(
    pool: &Arc<AnyPool>,
) -> HaliaResult<Arc<RwLock<Vec<Databoard>>>> {
    let db_databoards = persistence::databoard::read_databoards(pool).await?;
    let databoards: Arc<RwLock<Vec<Databoard>>> = Arc::new(RwLock::new(vec![]));
    for db_databoard in db_databoards {
        let databoard_id = Uuid::from_str(&db_databoard.id).unwrap();

        let db_datas = persistence::databoard::read_databoard_datas(pool, &databoard_id).await?;
        debug!("{}", db_datas.len());
        create_databoard(pool, &databoards, databoard_id, db_databoard.conf, false).await?;

        for db_data in db_datas {
            create_data(
                pool,
                &databoards,
                databoard_id,
                Uuid::from_str(&db_data.id).unwrap(),
                db_data.conf,
                false,
            )
            .await?;
        }
    }

    Ok(databoards)
}

pub async fn get_rule_info(
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    query: QueryRuleInfo,
) -> HaliaResult<SearchRuleInfo> {
    match databoards
        .read()
        .await
        .iter()
        .find(|databoard| databoard.id == query.databoard_id)
    {
        Some(databoard) => {
            let databoard_info = databoard.search();
            let data_info = databoard.search_data(&query.data_id).await?;
            Ok(SearchRuleInfo {
                databoard: databoard_info,
                data: data_info,
            })
        }
        None => Err(HaliaError::NotFound),
    }
}

pub async fn create_databoard(
    pool: &Arc<AnyPool>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateDataboardReq = serde_json::from_str(&body)?;
    let databoard = Databoard::new(id, req.base, req.ext)?;
    databoards.write().await.push(databoard);
    add_databoard_count();
    if persist {
        persistence::databoard::create_databoard(pool, &id, body).await?;
    }
    Ok(())
}

pub async fn search_databoards(
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    pagination: Pagination,
    query: QueryParams,
) -> SearchDataboardsResp {
    let mut data = vec![];
    let mut total = 0;

    for databoard in databoards.read().await.iter().rev() {
        let databoard = databoard.search();
        if let Some(name) = &query.name {
            if !databoard.conf.base.name.contains(name) {
                continue;
            }
        }

        if pagination.check(total) {
            data.push(databoard);
        }

        total += 1;
    }

    SearchDataboardsResp { total, data }
}

pub async fn update_databoard(
    pool: &Arc<AnyPool>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateDataboardReq = serde_json::from_str(&body)?;
    match databoards
        .write()
        .await
        .iter_mut()
        .find(|databoard| databoard.id == databoard_id)
    {
        Some(databoard) => databoard.update(req.base)?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::databoard::update_databoard(pool, &databoard_id, body).await?;

    Ok(())
}

pub async fn delete_databoard(
    pool: &Arc<AnyPool>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: Uuid,
) -> HaliaResult<()> {
    match databoards
        .write()
        .await
        .iter_mut()
        .find(|databoard| databoard.id == databoard_id)
    {
        Some(databoard) => databoard.delete()?,
        None => return Err(HaliaError::NotFound),
    }

    databoards
        .write()
        .await
        .retain(|databoard| databoard.id != databoard_id);
    sub_databoard_count();
    persistence::databoard::delete_databoard(pool, &databoard_id).await?;

    Ok(())
}

pub async fn create_data(
    pool: &Arc<AnyPool>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: Uuid,
    databoard_data_id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateDataReq = serde_json::from_str(&body)?;
    match databoards
        .write()
        .await
        .iter_mut()
        .find(|databoard| databoard.id == databoard_id)
    {
        Some(databoard) => {
            databoard
                .create_data(databoard_data_id.clone(), req)
                .await?
        }
        None => return Err(HaliaError::NotFound),
    }

    if persist {
        persistence::databoard::create_databoard_data(
            pool,
            &databoard_id,
            &databoard_data_id,
            body,
        )
        .await?;
    }

    Ok(())
}

pub async fn search_datas(
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: Uuid,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchDatasResp> {
    match databoards
        .read()
        .await
        .iter()
        .find(|databoard| databoard.id == databoard_id)
    {
        Some(device) => Ok(device.search_datas(pagination, query).await),
        None => Err(HaliaError::NotFound),
    }
}

pub async fn update_data(
    pool: &Arc<AnyPool>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: Uuid,
    databoard_data_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateDataReq = serde_json::from_str(&body)?;
    match databoards
        .write()
        .await
        .iter_mut()
        .find(|databoard| databoard.id == databoard_id)
    {
        Some(databoard) => databoard.update_data(databoard_data_id, req).await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::databoard::update_databoard_data(pool, &databoard_data_id, body).await?;

    Ok(())
}

pub async fn delete_data(
    pool: &Arc<AnyPool>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: Uuid,
    databoard_data_id: Uuid,
) -> HaliaResult<()> {
    match databoards
        .write()
        .await
        .iter_mut()
        .find(|databoard| databoard.id == databoard_id)
    {
        Some(databoard) => databoard.delete_data(databoard_data_id).await?,
        None => return Err(HaliaError::NotFound),
    }

    persistence::databoard::delete_databoard_data(pool, &databoard_data_id).await?;

    Ok(())
}

pub async fn add_data_ref(
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match databoards
        .write()
        .await
        .iter_mut()
        .find(|databoard| databoard.id == *databoard_id)
    {
        Some(databoard) => databoard.add_data_ref(&databoard_data_id, &rule_id).await,
        None => return Err(HaliaError::NotFound),
    }
}

pub async fn get_data_tx(
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    match databoards
        .write()
        .await
        .iter_mut()
        .find(|databoard| databoard.id == *databoard_id)
    {
        Some(databoard) => databoard.get_data_tx(&databoard_data_id, &rule_id).await,
        None => return Err(HaliaError::NotFound),
    }
}

pub async fn del_data_tx(
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match databoards
        .write()
        .await
        .iter_mut()
        .find(|databoard| databoard.id == *databoard_id)
    {
        Some(databoard) => databoard.del_data_tx(&databoard_data_id, &rule_id).await,
        None => return Err(HaliaError::NotFound),
    }
}

pub async fn del_data_ref(
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match databoards
        .write()
        .await
        .iter_mut()
        .find(|databoard| databoard.id == *databoard_id)
    {
        Some(databoard) => databoard.del_data_ref(&databoard_data_id, &rule_id).await,
        None => return Err(HaliaError::NotFound),
    }
}

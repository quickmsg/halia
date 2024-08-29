use std::sync::Arc;

use common::error::HaliaResult;
use databoard::Databoard;
use sqlx::AnyPool;
use tokio::sync::{Mutex, RwLock};
use types::databoard::CreateUpdateDataboardReq;
use uuid::Uuid;

mod data;
mod databoard;

pub async fn get_summary() -> HaliaResult<()> {
    todo!()
    // let mut total = 0;
    // let mut running_cnt = 0;
    // let mut err_cnt = 0;
    // let mut off_cnt = 0;
    // for app in self.apps.read().await.iter().rev() {
    //     let app = app.search().await;
    //     total += 1;

    //     if app.common.err.is_some() {
    //         err_cnt += 1;
    //     } else {
    //         if app.common.on {
    //             running_cnt += 1;
    //         } else {
    //             off_cnt += 1;
    //         }
    //     }
    // }
    // Summary {
    //     total,
    //     running_cnt,
    //     err_cnt,
    //     off_cnt,
    // }
}

pub async fn create_databoard(
    pool: &Arc<AnyPool>,
    databoards: &Arc<RwLock<Vec<Databoard>>>,
    id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateDataboardReq = serde_json::from_str(&body)?;
    let databoard = Databoard::new(id, req.base)?;
    if persist {}
    todo!()
}

pub async fn search_databoards() -> HaliaResult<()> {
    todo!()
}

pub async fn update_databoard() -> HaliaResult<()> {
    todo!()
}

pub async fn delete_databoard() -> HaliaResult<()> {
    todo!()
}

pub async fn create_data() -> HaliaResult<()> {
    todo!()
}

pub async fn search_datas() -> HaliaResult<()> {
    todo!()
}

pub async fn update_data() -> HaliaResult<()> {
    todo!()
}

pub async fn delete_data() -> HaliaResult<()> {
    todo!()
}

pub async fn add_data_ref() -> HaliaResult<()> {
    todo!()
}

pub async fn get_data_tx() -> HaliaResult<()> {
    todo!()
}

pub async fn del_data_tx() -> HaliaResult<()> {
    todo!()
}

pub async fn del_data_ref() -> HaliaResult<()> {
    todo!()
}

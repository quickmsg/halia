use std::sync::LazyLock;

use common::error::HaliaResult;
use databoard::Databoard;
use tokio::sync::RwLock;
use types::databoard::CreateUpdateDataboardReq;
use uuid::Uuid;

mod data;
mod databoard;

pub static GLOBAL_DATABOARD_MANAGER: LazyLock<DataboardManager> =
    LazyLock::new(|| DataboardManager {
        databoards: RwLock::new(vec![]),
    });

pub struct DataboardManager {
    databoards: RwLock<Vec<Databoard>>,
}

impl DataboardManager {
    pub async fn get_summary(&self) -> HaliaResult<()> {
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
        &self,
        id: Uuid,
        req: CreateUpdateDataboardReq,
        persist: bool,
    ) -> HaliaResult<()> {
        Databoard::new(id, req);
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
}

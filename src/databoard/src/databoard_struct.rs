use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::mpsc;
use types::databoard::{DataConf, DataboardConf, SearchDatasRuntimeResp};
use uuid::Uuid;

use crate::data::Data;

pub struct Databoard {
    conf: DataboardConf,
    datas: DashMap<Uuid, Data>,
}

impl Databoard {
    pub fn new(conf: DataboardConf) -> Self {
        Self {
            conf,
            datas: DashMap::new(),
        }
    }

    // pub fn check_duplicate(&self, base_conf: &BaseConf) -> HaliaResult<()> {
    //     if self.base_conf.name == base_conf.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     Ok(())
    // }

    // pub fn search(&self) -> SearchDataboardsItemResp {
    //     SearchDataboardsItemResp {
    //         id: self.id.clone(),
    //         conf: CreateUpdateDataboardReq {
    //             base: self.base_conf.clone(),
    //             ext: self.ext_conf.clone(),
    //         },
    //     }
    // }

    // pub fn update(&mut self, base_conf: BaseConf) -> HaliaResult<()> {
    //     self.base_conf = base_conf;
    //     Ok(())
    // }

    pub async fn stop(&mut self) {
        for mut data in self.datas.iter_mut() {
            data.stop().await;
        }
    }

    pub async fn create_data(&mut self, data_id: Uuid, conf: DataConf) -> HaliaResult<()> {
        let data = Data::new(conf).await?;
        self.datas.insert(data_id, data);

        Ok(())
    }

    // pub async fn search_datas(
    //     &self,
    //     pagination: Pagination,
    //     query: QueryParams,
    // ) -> SearchDatasResp {
    //     let mut total = 0;
    //     let mut datas = vec![];
    //     for (index, data) in self.datas.iter().rev().enumerate() {
    //         let data = data.search().await;
    //         if let Some(name) = &query.name {
    //             if !data.conf.base.name.contains(name) {
    //                 continue;
    //             }
    //         }

    //         if pagination.check(total) {
    //             unsafe {
    //                 datas.push(SearchDatasItemResp {
    //                     info: data,
    //                     rule_ref: self.data_ref_infos.get_unchecked(index).1.get_rule_ref(),
    //                 });
    //             }
    //         }

    //         total += 1;
    //     }

    //     SearchDatasResp { total, data: datas }
    // }

    pub async fn read_data_runtime(&self, data_id: &Uuid) -> HaliaResult<SearchDatasRuntimeResp> {
        Ok(self
            .datas
            .get(data_id)
            .ok_or(HaliaError::NotFound)?
            .read()
            .await)
    }

    pub async fn update_data(
        &mut self,
        data_id: Uuid,
        old_conf: DataConf,
        new_conf: DataConf,
    ) -> HaliaResult<()> {
        // for data in self.datas.iter() {
        //     if data.id != data_id {
        //         data.check_duplicate(&req)?;
        //     }
        // }
        Ok(self
            .datas
            .get_mut(&data_id)
            .ok_or(HaliaError::NotFound)?
            .update(old_conf, new_conf)
            .await)
    }

    pub async fn delete_data(&mut self, data_id: Uuid) -> HaliaResult<()> {
        self.datas
            .get_mut(&data_id)
            .ok_or(HaliaError::NotFound)?
            .stop()
            .await;
        self.datas.remove(&data_id);
        Ok(())
    }

    pub async fn get_data_tx(&mut self, data_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        Ok(self
            .datas
            .get(&data_id)
            .ok_or(HaliaError::NotFound)?
            .mb_tx
            .clone())
    }
}

use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use tokio::sync::mpsc;
use types::{
    databoard::{
        CreateUpdateDataReq, CreateUpdateDataboardReq, DataboardConf, SearchDataboardsItemResp,
        SearchDatasInfoResp,
    },
    BaseConf,
};
use uuid::Uuid;

use crate::data::Data;

pub struct Databoard {
    pub id: Uuid,
    base_conf: BaseConf,
    ext_conf: DataboardConf,
    pub datas: Vec<Data>,
}

impl Databoard {
    pub fn new(id: Uuid, base_conf: BaseConf, ext_conf: DataboardConf) -> HaliaResult<Self> {
        Ok(Self {
            id,
            base_conf,
            ext_conf,
            datas: vec![],
        })
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchDataboardsItemResp {
        SearchDataboardsItemResp {
            id: self.id.clone(),
            conf: CreateUpdateDataboardReq {
                base: self.base_conf.clone(),
                ext: self.ext_conf.clone(),
            },
        }
    }

    pub fn update(&mut self, base_conf: BaseConf) -> HaliaResult<()> {
        self.base_conf = base_conf;
        Ok(())
    }

    pub fn delete(&mut self) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn create_data(
        &mut self,
        data_id: Uuid,
        req: CreateUpdateDataReq,
    ) -> HaliaResult<()> {
        for data in self.datas.iter() {
            data.check_duplicate(&req)?;
        }

        let data = Data::new(data_id, req).await?;
        self.datas.push(data);

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

    pub async fn search_data(&self, data_id: &Uuid) -> HaliaResult<SearchDatasInfoResp> {
        match self.datas.iter().find(|data| data.id == *data_id) {
            Some(data) => Ok(data.search().await),
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn update_data(
        &mut self,
        data_id: Uuid,
        req: CreateUpdateDataReq,
    ) -> HaliaResult<()> {
        for data in self.datas.iter() {
            if data.id != data_id {
                data.check_duplicate(&req)?;
            }
        }

        match self.datas.iter_mut().find(|data| data.id == data_id) {
            Some(data) => data.update(req).await,
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_data(&mut self, data_id: Uuid) -> HaliaResult<()> {
        match self.datas.iter_mut().find(|data| data.id == data_id) {
            Some(source) => source.stop().await,
            None => unreachable!(),
        }

        self.datas.retain(|data| data.id != data_id);

        Ok(())
    }

    pub async fn get_data_tx(&mut self, data_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.datas.iter_mut().find(|data| data.id == *data_id) {
            Some(data) => Ok(data.mb_tx.clone()),
            None => unreachable!(),
        }
    }
}

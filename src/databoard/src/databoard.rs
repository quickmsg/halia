use common::{
    active_ref, add_ref, check_delete, deactive_ref, del_ref,
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use message::MessageBatch;
use paste::paste;
use tokio::sync::mpsc;
use types::{
    databoard::{
        CreateUpdateDataReq, CreateUpdateDataboardReq, DataboardConf, QueryParams,
        SearchDataboardsItemResp, SearchDatasInfoResp, SearchDatasItemResp, SearchDatasResp,
    },
    BaseConf, Pagination,
};
use uuid::Uuid;

use crate::data::Data;

pub struct Databoard {
    pub id: Uuid,
    base_conf: BaseConf,
    ext_conf: DataboardConf,
    pub datas: Vec<Data>,
    pub data_ref_infos: Vec<(Uuid, RefInfo)>,
}

impl Databoard {
    pub fn new(id: Uuid, base_conf: BaseConf, ext_conf: DataboardConf) -> HaliaResult<Self> {
        Ok(Self {
            id,
            base_conf,
            ext_conf,
            datas: vec![],
            data_ref_infos: vec![],
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
        todo!()
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
        self.data_ref_infos.push((data_id, RefInfo::new()));

        Ok(())
    }

    pub async fn search_datas(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchDatasResp {
        let mut total = 0;
        let mut datas = vec![];
        for (index, data) in self.datas.iter().rev().enumerate() {
            let data = data.search().await;
            if let Some(name) = &query.name {
                if !data.conf.base.name.contains(name) {
                    continue;
                }
            }

            if pagination.check(total) {
                unsafe {
                    datas.push(SearchDatasItemResp {
                        info: data,
                        rule_ref: self.data_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    });
                }
            }

            total += 1;
        }

        SearchDatasResp { total, data: datas }
    }

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
        check_delete!(self, data, data_id);
        match self.datas.iter_mut().find(|data| data.id == data_id) {
            Some(source) => source.stop().await,
            None => unreachable!(),
        }

        self.datas.retain(|data| data.id != data_id);
        self.data_ref_infos.retain(|(id, _)| *id != data_id);

        Ok(())
    }

    pub async fn add_data_ref(&mut self, data_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        add_ref!(self, data, data_id, rule_id)
    }

    pub async fn get_data_tx(
        &mut self,
        data_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        active_ref!(self, data, data_id, rule_id);
        match self.datas.iter_mut().find(|data| data.id == *data_id) {
            Some(data) => Ok(data.mb_tx.clone()),
            None => unreachable!(),
        }
    }

    pub async fn del_data_tx(&mut self, data_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        deactive_ref!(self, data, data_id, rule_id)
    }

    pub async fn del_data_ref(&mut self, data_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_ref!(self, data, data_id, rule_id)
    }
}

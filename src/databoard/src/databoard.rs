use common::{
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use types::{
    databoard::{CreateUpdateDataReq, SearchDataboardsItemResp},
    rules::QueryParams,
    BaseConf, Pagination,
};
use uuid::Uuid;

use crate::data::Data;

pub struct Databoard {
    pub id: Uuid,
    base_conf: BaseConf,
    // ext_conf: DataboardConf,
    pub datas: Vec<Data>,
    pub data_ref_infos: Vec<(Uuid, RefInfo)>,
}

impl Databoard {
    pub fn new(id: Uuid, base_conf: BaseConf) -> HaliaResult<Self> {
        Ok(Self {
            id,
            base_conf: base_conf,
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
            conf: CreateUpdateDataReq {
                base: self.base_conf.clone(),
            },
        }
    }

    pub fn update(&mut self, base_conf: BaseConf) -> HaliaResult<()> {
        self.base_conf = base_conf;
        Ok(())
    }

    pub fn delete(&mut self, base_conf: BaseConf) -> HaliaResult<()> {
        todo!()
    }

    pub fn create_data(&mut self, data_id: Uuid, req: CreateUpdateDataReq) -> HaliaResult<()> {
        todo!()
    }

    pub async fn search_datas(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn update_data(
        &mut self,
        data_id: Uuid,
        req: CreateUpdateDataReq,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete_data(&mut self, data_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn add_data_ref(&mut self, data_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn get_data_tx(&mut self, data_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn del_data_tx(&mut self, data_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn del_data_ref(&mut self, data_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }
}

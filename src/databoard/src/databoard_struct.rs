use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use tokio::sync::mpsc::UnboundedSender;
use types::databoard::{DataConf, SearchDatasRuntimeResp};

use crate::data::Data;

pub struct Databoard {
    datas: DashMap<String, Data>,
}

impl Databoard {
    pub fn new() -> Self {
        Self {
            datas: DashMap::new(),
        }
    }

    pub async fn stop(&mut self) {
        for mut data in self.datas.iter_mut() {
            data.stop().await;
        }
    }

    pub async fn create_data(&mut self, data_id: String, conf: DataConf) -> HaliaResult<()> {
        let data = Data::new(conf);
        self.datas.insert(data_id, data);

        Ok(())
    }

    pub async fn read_data_runtime(&self, data_id: &String) -> HaliaResult<SearchDatasRuntimeResp> {
        match self.datas.get(data_id) {
            Some(data) => Ok(data.read().await),
            None => Err(HaliaError::NotFound(data_id.to_string())),
        }
    }

    pub async fn update_data(
        &mut self,
        data_id: String,
        old_conf: DataConf,
        new_conf: DataConf,
    ) -> HaliaResult<()> {
        match self.datas.get_mut(&data_id) {
            Some(mut data) => {
                data.update(old_conf, new_conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(data_id)),
        }
    }

    pub async fn delete_data(&self, data_id: &String) -> HaliaResult<()> {
        match self.datas.remove(data_id) {
            Some((_, mut data)) => {
                data.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(data_id.to_string())),
        }
    }

    pub async fn get_data_txs(
        &self,
        data_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<UnboundedSender<RuleMessageBatch>>> {
        match self.datas.get(data_id) {
            Some(data) => Ok(data.get_txs(cnt)),
            None => Err(HaliaError::NotFound(data_id.to_string())),
        }
    }
}

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::mpsc;
use types::databoard::{DataConf, DataboardConf, SearchDatasRuntimeResp};

use crate::data::Data;

pub struct Databoard {
    _conf: DataboardConf,
    datas: DashMap<String, Data>,
}

impl Databoard {
    pub fn new(conf: DataboardConf) -> Self {
        Self {
            _conf: conf,
            datas: DashMap::new(),
        }
    }

    pub async fn stop(&mut self) {
        for mut data in self.datas.iter_mut() {
            data.stop().await;
        }
    }

    pub async fn create_data(&mut self, data_id: String, conf: DataConf) -> HaliaResult<()> {
        let data = Data::new(conf).await?;
        self.datas.insert(data_id, data);

        Ok(())
    }

    pub async fn get_data_value(&self, data_id: &String) -> HaliaResult<serde_json::Value> {
        match self.datas.get(data_id) {
            Some(data) => Ok(data.value.read().await.clone()),
            None => Err(HaliaError::NotFound(data_id.to_string())),
        }
    }

    pub async fn read_data_runtime(&self, data_id: &String) -> HaliaResult<SearchDatasRuntimeResp> {
        Ok(self
            .datas
            .get(data_id)
            .ok_or(HaliaError::NotFound(data_id.to_string()))?
            .read()
            .await)
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
        match self.datas.get_mut(data_id) {
            Some(mut data) => {
                data.stop().await;
                self.datas.remove(data_id);
                Ok(())
            }
            None => Err(HaliaError::NotFound(data_id.to_string())),
        }
    }

    pub async fn get_data_tx(&self, data_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.datas.get(data_id) {
            Some(data) => Ok(data.mb_tx.clone()),
            None => Err(HaliaError::NotFound(data_id.to_string())),
        }
    }
}

use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use rumqttc::valid_filter;
use tokio::sync::broadcast;
use types::{
    apps::mqtt_client::SourceConf, BaseConf, CreateUpdateSourceOrSinkReq,
    SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    pub base_conf: BaseConf,
    pub ext_conf: SourceConf,
    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Source {
    pub fn new(id: Uuid, base_conf: BaseConf, ext_conf: SourceConf) -> Self {
        Source {
            id,
            base_conf,
            ext_conf,
            mb_tx: None,
        }
    }

    pub fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
        if !valid_filter(&conf.topic) {
            return Err(HaliaError::Common("topic错误!".to_owned()));
        }

        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, ext_conf: &SourceConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        if self.ext_conf.topic == ext_conf.topic {
            return Err(HaliaError::Common("主题重复！".to_owned()));
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        todo!()
    }

    pub fn update(&mut self, base_conf: BaseConf, ext_conf: SourceConf) -> bool {
        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        restart
    }

    pub fn start(&mut self) {
        let (mb_tx, _) = broadcast::channel(16);
        self.mb_tx = Some(mb_tx);
    }
}

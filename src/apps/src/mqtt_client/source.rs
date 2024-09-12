use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use rumqttc::valid_filter;
use tokio::sync::broadcast;
use types::{apps::mqtt_client::SourceConf, BaseConf, SearchSourcesOrSinksInfoResp};

pub struct Source {
    pub conf: SourceConf,
    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Source {
    pub fn new(conf: SourceConf) -> Self {
        let (mb_tx, _) = broadcast::channel(16);
        Source { conf, mb_tx }
    }

    pub fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
        if !valid_filter(&conf.topic) {
            return Err(HaliaError::Common("topic错误!".to_owned()));
        }

        Ok(())
    }

    // pub fn check_duplicate(&self, base_conf: &BaseConf, ext_conf: &SourceConf) -> HaliaResult<()> {
    //     if self.base_conf.name == base_conf.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     if self.ext_conf.topic == ext_conf.topic {
    //         return Err(HaliaError::Common("主题重复！".to_owned()));
    //     }

    //     Ok(())
    // }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        todo!()
    }

    pub fn update(&mut self, base_conf: BaseConf, ext_conf: SourceConf) -> bool {
        todo!()
        // let mut restart = false;
        // if self.ext_conf != ext_conf {
        //     restart = true;
        // }
        // self.base_conf = base_conf;
        // self.ext_conf = ext_conf;

        // restart
    }
}

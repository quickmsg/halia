use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_item_resp,
    ref_info::RefInfo,
};
use message::MessageBatch;
use rumqttc::valid_filter;
use tokio::sync::broadcast;
use types::{
    apps::mqtt_client::SourceConf, BaseConf, CreateUpdateSourceOrSinkReq,
    SearchSourcesOrSinksItemResp,
};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    pub base_conf: BaseConf,
    pub ext_conf: SourceConf,
    pub ref_info: RefInfo,
    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Source {
    pub fn new(source_id: Uuid, base_conf: BaseConf, ext_conf: SourceConf) -> Self {
        Source {
            id: source_id,
            base_conf,
            ext_conf,
            mb_tx: None,
            ref_info: RefInfo::new(),
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

    pub fn search(&self) -> SearchSourcesOrSinksItemResp {
        get_search_sources_or_sinks_item_resp!(self)
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

    pub fn get_mb_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.mb_tx.as_ref().unwrap().subscribe()
    }
}

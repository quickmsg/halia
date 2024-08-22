use common::{
    error::{HaliaError, HaliaResult},
    get_id, persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use rumqttc::valid_topic;
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
    pub async fn new(
        app_id: &Uuid,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<Self> {
        let (base_conf, ext_conf, data) = Source::parse_conf(req)?;

        let (source_id, new) = get_id(source_id);
        if new {
            persistence::create_source(app_id, &source_id, &data).await?;
        }

        Ok(Source {
            id: source_id,
            base_conf,
            ext_conf,
            mb_tx: None,
            ref_info: RefInfo::new(),
        })
    }

    fn parse_conf(req: CreateUpdateSourceOrSinkReq) -> HaliaResult<(BaseConf, SourceConf, String)> {
        let data = serde_json::to_string(&req)?;
        let conf: SourceConf = serde_json::from_value(req.ext)?;

        // TODO 其他检查

        if !valid_topic(&conf.topic) {
            return Err(HaliaError::Common("topic不合法".to_owned()));
        }

        Ok((req.base, conf, data))
    }

    // pub fn check_duplicate(&self, req: &CreateUpdateSourceReq) -> HaliaResult<()> {
    //     if self.conf.base.name == req.base.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     if self.conf.ext.topic == req.ext.topic {
    //         return Err(HaliaError::Common("主题重复！".to_owned()));
    //     }

    //     Ok(())
    // }

    pub fn search(&self) -> SearchSourcesOrSinksItemResp {
        SearchSourcesOrSinksItemResp {
            id: self.id.clone(),
            conf: CreateUpdateSourceOrSinkReq {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(self.ext_conf.clone()).unwrap(),
            },
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn update(
        &mut self,
        app_id: &Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<bool> {
        let (base_conf, ext_conf, data) = Source::parse_conf(req)?;

        persistence::update_source(app_id, &self.id, &data).await?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        Ok(restart)
    }

    pub async fn delete(&self, app_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::DeleteRefing);
        }
        persistence::delete_source(app_id, &self.id).await?;

        Ok(())
    }

    pub fn get_mb_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        match &self.mb_tx {
            Some(mb_tx) => mb_tx.subscribe(),
            None => {
                let (mb_tx, mb_rx) = broadcast::channel(16);
                self.mb_tx = Some(mb_tx);
                mb_rx
            }
        }
    }

    pub fn del_mb_rx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id);
        if self.ref_info.can_stop() {
            self.mb_tx = None;
        }
    }
}

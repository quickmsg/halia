use common::{
    error::{HaliaError, HaliaResult},
    get_id, persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use tokio::sync::broadcast;
use types::{
    apps::mqtt_client::{CreateUpdateSourceReq, SearchSourcesItemResp},
    RuleRef,
};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    pub conf: CreateUpdateSourceReq,

    pub ref_info: RefInfo,
    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Source {
    pub async fn new(
        app_id: &Uuid,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceReq,
    ) -> HaliaResult<Self> {
        let (source_id, new) = get_id(source_id);
        if new {
            persistence::apps::mqtt_client::create_source(
                app_id,
                &source_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Source {
            id: source_id,
            conf: req,
            mb_tx: None,
            ref_info: RefInfo::new(),
        })
    }

    pub fn search(&self) -> SearchSourcesItemResp {
        SearchSourcesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            rule_ref: RuleRef {
                rule_ref_cnt: self.ref_info.ref_cnt(),
                rule_active_ref_cnt: self.ref_info.active_ref_cnt(),
            },
        }
    }

    pub async fn update(&mut self, app_id: &Uuid, req: CreateUpdateSourceReq) -> HaliaResult<bool> {
        persistence::apps::mqtt_client::update_source(
            app_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }

        self.conf = req;

        Ok(restart)
    }

    pub async fn delete(&self, app_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::DeleteRefing);
        }
        persistence::apps::mqtt_client::delete_source(app_id, &self.id).await?;

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

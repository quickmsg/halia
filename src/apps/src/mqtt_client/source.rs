use common::{
    error::{HaliaError, HaliaResult},
    persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use tokio::sync::broadcast;
use types::apps::mqtt_client::{CreateUpdateSourceReq, SearchSourcesItemResp};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    pub conf: CreateUpdateSourceReq,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,

    ref_info: RefInfo,
}

impl Source {
    pub async fn new(
        app_id: &Uuid,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceReq,
    ) -> HaliaResult<Self> {
        let (source_id, new) = match source_id {
            Some(source_id) => (source_id, false),
            None => (Uuid::new_v4(), true),
        };

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
            tx: None,
            ref_cnt: 0,
            ref_info: RefInfo::new(),
        })
    }

    pub fn search(&self) -> SearchSourcesItemResp {
        SearchSourcesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
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
        if self.ref_cnt > 0 {
            // TODO
            return Err(HaliaError::NotFound);
        }
        persistence::apps::mqtt_client::delete_source(app_id, &self.id).await?;

        Ok(())
    }

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.add_ref(rule_id);
    }

    pub fn get_mb_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        // self.ref_info.subscribe(rule_id)
        todo!()
    }

    pub fn del_mb_rx(&mut self, rule_id: &Uuid) {
        // self.ref_info.unsubscribe(rule_id)
        todo!()
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.del_ref(rule_id)
    }
}

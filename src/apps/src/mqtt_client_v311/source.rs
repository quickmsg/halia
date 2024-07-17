use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use tokio::sync::broadcast;
use types::apps::mqtt_client_v311::{CreateUpdateSourceReq, SearchSourcesItemResp};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    pub conf: CreateUpdateSourceReq,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,
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
        if self.conf.topic != req.topic || self.conf.qos != req.qos {
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

    pub fn subscribe(&mut self) -> broadcast::Receiver<MessageBatch> {
        self.ref_cnt += 1;
        match &self.tx {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel::<MessageBatch>(16);
                self.tx = Some(tx);
                rx
            }
        }
    }

    pub fn unsubscribe(&mut self) {
        self.ref_cnt -= 1;
        if self.ref_cnt == 0 {
            self.tx = None;
        }
    }
}

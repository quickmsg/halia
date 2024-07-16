use common::{error::HaliaResult, persistence};
use message::MessageBatch;
use tokio::sync::broadcast;
use types::apps::mqtt_client::{CreateUpdateSourceReq, SearchSourcesItemResp};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    pub conf: CreateUpdateSourceReq,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: u8,
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

    pub async fn delete(&mut self, app_id: &Uuid) -> HaliaResult<()> {
        // TODO 判断引用，是否可以删除
        persistence::apps::mqtt_client::delete_source(app_id, &self.id).await?;

        Ok(())
    }
}

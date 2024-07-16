use common::{error::HaliaResult, persistence};
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    pub conf: Conf,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: u8,
}

#[derive(Deserialize, Serialize)]
pub struct Conf {
    pub name: String,
    pub topic: String,
    pub qos: u8,
    pub desc: Option<String>,
}

impl Source {
    pub async fn new(app_id: &Uuid, source_id: Option<Uuid>, data: String) -> HaliaResult<Self> {
        let conf: Conf = serde_json::from_str(&data)?;

        let (source_id, new) = match source_id {
            Some(source_id) => (source_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::apps::mqtt_client::create_source(app_id, &source_id, &data).await?;
        }

        Ok(Source {
            id: source_id,
            conf,
            tx: None,
            ref_cnt: 0,
        })
    }

    pub fn search(&self) {}

    pub async fn update(&mut self, app_id: &Uuid, data: String) -> HaliaResult<bool> {
        let conf: Conf = serde_json::from_str(&data)?;

        persistence::apps::mqtt_client::update_source(app_id, &self.id, &data).await?;

        let mut restart = false;
        if self.conf.topic != conf.topic || self.conf.qos != conf.qos {
            restart = true;
        }

        self.conf = conf;

        Ok(restart)
    }

    pub async fn delete(&mut self, app_id: &Uuid) -> HaliaResult<()> {
        // TODO 判断引用，是否可以删除
        persistence::apps::mqtt_client::delete_source(app_id, &self.id).await?;

        Ok(())
    }
}

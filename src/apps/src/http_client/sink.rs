use common::{error::HaliaResult, persistence, ref_info::RefInfo};
use types::apps::http_client::{CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    conf: CreateUpdateSinkReq,

    ref_info: RefInfo,
}

pub async fn new(
    app_id: &Uuid,
    sink_id: Option<Uuid>,
    req: CreateUpdateSinkReq,
) -> HaliaResult<Sink> {
    let (sink_id, new) = match sink_id {
        Some(sink_id) => (sink_id, false),
        None => (Uuid::new_v4(), true),
    };

    if new {
        persistence::apps::http_client::create_sink(
            app_id,
            &sink_id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;
    }

    Ok(Sink {
        id: sink_id,
        conf: req,
        ref_info: RefInfo::new(),
    })
}

impl Sink {
    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, app_id: &Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        persistence::apps::http_client::update_sink(
            app_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        todo!()
    }

    pub async fn delete(&mut self, app_id: &Uuid) -> HaliaResult<()> {
        persistence::apps::http_client::delete_sink(app_id, &self.id).await?;
        Ok(())
    }

    pub async fn start() {}

    pub async fn stop() {}
}

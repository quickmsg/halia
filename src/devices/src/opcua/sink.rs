use common::{error::HaliaResult, get_id, persistence, ref_info::RefInfo};
use types::devices::opcua::{CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    conf: CreateUpdateSinkReq,

    ref_info: RefInfo,
}

impl Sink {
    pub async fn new(
        device_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<Self> {
        let (sink_id, new) = get_id(sink_id);

        if new {
            // persistence::devices::opcua::create_sink(
            //     device_id,
            //     &sink_id,
            //     serde_json::to_string(&req).unwrap(),
            // )
            // .await?;
        }

        todo!()
    }

    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }
}

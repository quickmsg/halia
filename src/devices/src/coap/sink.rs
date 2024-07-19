use common::error::HaliaResult;
use types::devices::coap::CreateUpdateSinkReq;
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
}

impl Sink {
    pub async fn new(
        device_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<Self> {
        let (sink_id, new) = match sink_id {
            Some(sink_id) => (sink_id, false),
            None => (Uuid::new_v4(), true),
        };
        todo!()
    }
}

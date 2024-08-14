use std::sync::mpsc;

use common::{error::HaliaResult, get_id, ref_info::RefInfo};
use message::MessageBatch;
use tokio::sync::broadcast;
use types::devices::coap::CreateUpdateObserveReq;
use uuid::Uuid;

pub struct Observe {
    pub id: Uuid,
    conf: CreateUpdateObserveReq,

    on: bool,
    stop_signal_tx: Option<mpsc::Sender<()>>,

    ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Observe {
    pub async fn new(
        device_id: &Uuid,
        observe_id: Option<Uuid>,
        req: CreateUpdateObserveReq,
    ) -> HaliaResult<Self> {
        let (observe_id, new) = get_id(observe_id);
        if new {}
        todo!()
    }

    fn check_conf(req: &CreateUpdateObserveReq) -> HaliaResult<()> {
        todo!()
    }

    pub fn check_duplicate(&self, req: &CreateUpdateObserveReq) -> HaliaResult<()> {
        todo!()
    }

    pub fn search(&self) {}

    pub async fn update(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub fn can_stop(&self) -> bool {
        self.ref_info.can_stop()
    }

    pub fn can_delete(&self) -> bool {
        self.ref_info.can_delete()
    }
}

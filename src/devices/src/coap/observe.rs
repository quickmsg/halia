use std::sync::mpsc;

use common::{error::HaliaResult, get_id, persistence, ref_info::RefInfo};
use message::MessageBatch;
use tokio::sync::broadcast;
use types::devices::coap::{CreateUpdateObserveReq, SearchObservesItemResp};
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
        Self::check_conf(&req)?;

        let (observe_id, new) = get_id(observe_id);
        if new {
            persistence::devices::coap::create_observe(
                device_id,
                &observe_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: observe_id,
            conf: req,
            on: false,
            stop_signal_tx: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
        })
    }

    fn check_conf(req: &CreateUpdateObserveReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateObserveReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn search(&self) -> SearchObservesItemResp {
        SearchObservesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        req: CreateUpdateObserveReq,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }
}

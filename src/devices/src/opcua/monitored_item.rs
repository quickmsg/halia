use common::{
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use message::MessageBatch;
use tokio::sync::broadcast;
use types::devices::opcua::{CreateUpdateMonitoredItemReq, SearchMonitoredItemsItemResp};
use uuid::Uuid;

pub struct MonitoredItem {
    pub id: Uuid,
    conf: CreateUpdateMonitoredItemReq,

    on: bool,

    pub ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl MonitoredItem {
    pub async fn new(
        device_id: &Uuid,
        subscription_id: Uuid,
        req: CreateUpdateMonitoredItemReq,
    ) -> HaliaResult<Self> {
        Self::check_conf(&req)?;

        Ok(Self {
            id: subscription_id,
            conf: req,
            on: false,
            ref_info: RefInfo::new(),
            mb_tx: None,
        })
    }

    fn check_conf(req: &CreateUpdateMonitoredItemReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateMonitoredItemReq) -> HaliaResult<()> {
        if self.conf.base.name == req.base.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchMonitoredItemsItemResp {
        SearchMonitoredItemsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        req: CreateUpdateMonitoredItemReq,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::DeleteRefing);
        }

        Ok(())
    }
}

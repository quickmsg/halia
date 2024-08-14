use common::{
    check_and_set_on_false, check_and_set_on_true, error::HaliaResult, ref_info::RefInfo,
};
use uuid::Uuid;

pub struct Event {
    pub id: Uuid,
    on: bool,
    err: Option<String>,

    ref_info: RefInfo,
}

impl Event {
    pub async fn new(device_id: &Uuid, event_id: Option<Uuid>) -> HaliaResult<Self> {
        todo!()
    }

    pub fn search(&self) {}

    pub fn update(&mut self) {}

    pub fn delete(&mut self) {}

    pub fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);
        todo!()
    }

    pub fn stop(&mut self) -> HaliaResult<()> {
        check_and_set_on_false!(self);
        todo!()
    }

    pub fn can_stop(&self) -> bool {
        self.ref_info.can_stop()
    }

    pub fn can_delete(&self) -> bool {
        self.ref_info.can_delete()
    }
}

use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id, persistence,
    ref_info::RefInfo,
};
use types::devices::opcua::{CreateUpdateEventReq, SearchEventsItemResp};
use uuid::Uuid;

pub struct Event {
    pub id: Uuid,
    conf: CreateUpdateEventReq,

    on: bool,
    err: Option<String>,

    pub ref_info: RefInfo,
}

impl Event {
    pub async fn new(
        device_id: &Uuid,
        event_id: Option<Uuid>,
        req: CreateUpdateEventReq,
    ) -> HaliaResult<Self> {
        Self::check_conf(&req)?;

        let (event_id, new) = get_id(event_id);
        if new {
            persistence::devices::opcua::create_event(
                device_id,
                &event_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: event_id,
            conf: req,
            on: false,
            err: None,
            ref_info: RefInfo::new(),
        })
    }

    fn check_conf(req: &CreateUpdateEventReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateEventReq) -> HaliaResult<()> {
        if self.conf.base.name == req.base.name {
            return Err(HaliaError::NameExists);
        }

        // if self.conf.ext.data_type == req.ext.data_type
        //     && self.conf.ext.slave == req.ext.slave
        //     && self.conf.ext.area == req.ext.area
        //     && self.conf.ext.address == req.ext.address
        // {
        //     return Err(HaliaError::AddressExists);
        // }

        Ok(())
    }

    pub fn search(&self) -> SearchEventsItemResp {
        SearchEventsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateEventReq) -> HaliaResult<()> {
        Self::check_conf(&req)?;

        persistence::devices::opcua::update_event(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        Ok(())
    }

    pub fn delete(&mut self) {}

    pub fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);
        todo!()
    }

    pub fn stop(&mut self) -> HaliaResult<()> {
        check_and_set_on_false!(self);
        todo!()
    }
}

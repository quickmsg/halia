use common::error::HaliaResult;
use opcua::types::{DataValue, Variant};
use types::devices::opcua::{CreateUpdateGroupVariableReq, SearchGroupVariablesItemResp};
use uuid::Uuid;

#[derive(Debug)]
pub struct Variable {
    pub id: Uuid,
    pub conf: CreateUpdateGroupVariableReq,
    pub value: Option<Variant>,
}

impl Variable {
    pub async fn new(
        device_id: &Uuid,
        group_id: &Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<Self> {
        let (variable_id, new) = match variable_id {
            Some(variable_id) => (variable_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {}

        Ok(Self {
            id: variable_id,
            conf: req,
            value: todo!(),
        })
    }

    pub fn search(&self) -> SearchGroupVariablesItemResp {
        SearchGroupVariablesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, req: CreateUpdateGroupVariableReq) -> HaliaResult<()> {
        todo!()
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        todo!()
    }

    pub fn write(&mut self, data_value: Option<DataValue>) {
        match data_value {
            Some(data_value) => self.value = data_value.value,
            None => self.value = None,
        }
    }
}

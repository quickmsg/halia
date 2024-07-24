use common::{error::HaliaResult, persistence};
use opcua::types::{
    ByteString, Guid, Identifier, NodeId, QualifiedName, ReadValueId, UAString, Variant,
};
use types::devices::opcua::{
    CreateUpdateGroupVariableReq, SearchGroupVariablesItemResp, VariableConf,
};
use uuid::Uuid;

pub struct Variable {
    pub id: Uuid,
    conf: CreateUpdateGroupVariableReq,
    pub value: Option<Variant>,
}

impl Variable {
    pub async fn new(
        device_id: &Uuid,
        group_id: &Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<(Self, ReadValueId)> {
        let (variable_id, new) = match variable_id {
            Some(variable_id) => (variable_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::opcua::create_group_variable(
                device_id,
                group_id,
                &variable_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        let read_value_id = Variable::get_read_value_id(&req.variable_conf);

        Ok((
            Self {
                id: variable_id,
                conf: req,
                value: None,
            },
            read_value_id,
        ))
    }

    pub fn get_read_value_id(variable_conf: &VariableConf) -> ReadValueId {
        let namespace = variable_conf.namespace;
        let identifier = match variable_conf.identifier_type {
            types::devices::opcua::IdentifierType::Numeric => {
                let num: u32 =
                    serde_json::from_value::<u32>(variable_conf.identifier.clone()).unwrap();
                Identifier::Numeric(num)
            }
            types::devices::opcua::IdentifierType::String => {
                let s: UAString = serde_json::from_value(variable_conf.identifier.clone()).unwrap();
                Identifier::String(s)
            }
            types::devices::opcua::IdentifierType::Guid => {
                let guid: Guid = serde_json::from_value(variable_conf.identifier.clone()).unwrap();
                Identifier::Guid(guid)
            }
            types::devices::opcua::IdentifierType::ByteString => {
                let bs: ByteString =
                    serde_json::from_value(variable_conf.identifier.clone()).unwrap();
                Identifier::ByteString(bs)
            }
        };

        ReadValueId {
            node_id: NodeId {
                namespace,
                identifier,
            },
            attribute_id: 13,
            index_range: UAString::null(),
            data_encoding: QualifiedName::null(),
        }
    }

    pub fn search(&self) -> SearchGroupVariablesItemResp {
        SearchGroupVariablesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            value: serde_json::to_value(&self.value).unwrap(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        group_id: &Uuid,
        req: CreateUpdateGroupVariableReq,
    ) -> HaliaResult<Option<ReadValueId>> {
        persistence::devices::opcua::update_group_variable(
            device_id,
            group_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.variable_conf != req.variable_conf {
            restart = true;
        }
        self.conf = req;

        if restart {
            Ok(Some(Variable::get_read_value_id(&self.conf.variable_conf)))
        } else {
            Ok(None)
        }
    }

    pub async fn delete(&self, device_id: &Uuid, group_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::opcua::delete_group_variable(device_id, group_id, &self.id).await?;
        Ok(())
    }
}

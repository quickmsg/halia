use common::{error::HaliaResult, get_id, persistence};
use opcua::types::{
    ByteString, Guid, Identifier, NodeId, QualifiedName, ReadValueId, UAString, Variant,
};
use types::devices::opcua::{CreateUpdateVariableReq, SearchVariablesItemResp, VariableConf};
use uuid::Uuid;

pub struct Variable {
    pub id: Uuid,
    conf: CreateUpdateVariableReq,
    pub value: Option<Variant>,
}

impl Variable {
    pub async fn new(
        device_id: &Uuid,
        group_id: &Uuid,
        variable_id: Option<Uuid>,
        req: CreateUpdateVariableReq,
    ) -> HaliaResult<(Self, ReadValueId)> {
        Self::check_conf(&req)?;

        let (variable_id, new) = get_id(variable_id);
        if new {
            persistence::devices::opcua::create_group_variable(
                device_id,
                group_id,
                &variable_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        let read_value_id = Variable::get_read_value_id(&req.ext);

        Ok((
            Self {
                id: variable_id,
                conf: req,
                value: None,
            },
            read_value_id,
        ))
    }

    fn check_conf(req: &CreateUpdateVariableReq) -> HaliaResult<()> {
        Ok(())
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

    pub fn search(&self) -> SearchVariablesItemResp {
        SearchVariablesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            value: serde_json::to_value(&self.value).unwrap(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        group_id: &Uuid,
        req: CreateUpdateVariableReq,
    ) -> HaliaResult<Option<ReadValueId>> {
        persistence::devices::opcua::update_group_variable(
            device_id,
            group_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

        if restart {
            Ok(Some(Variable::get_read_value_id(&self.conf.ext)))
        } else {
            Ok(None)
        }
    }

    pub async fn delete(&self, device_id: &Uuid, group_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::opcua::delete_group_variable(device_id, group_id, &self.id).await?;
        Ok(())
    }
}

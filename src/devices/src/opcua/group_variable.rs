use std::sync::Arc;

use common::{error::HaliaResult, persistence};
use opcua::{
    client::Session,
    types::{
        ByteString, Guid, Identifier, NodeId, QualifiedName, ReadValueId, TimestampsToReturn,
        UAString, Variant,
    },
};
use tokio::sync::RwLock;
use tracing::debug;
use types::devices::opcua::{CreateUpdateGroupVariableReq, SearchGroupVariablesItemResp};
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
    ) -> HaliaResult<Self> {
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

        Ok(Self {
            id: variable_id,
            conf: req,
            value: None,
        })
    }

    pub fn get_read_value_id(&self) -> ReadValueId {
        let namespace = self.conf.variable_conf.namespace;
        let identifier = match &self.conf.variable_conf.identifier_type {
            types::devices::opcua::IdentifierType::Numeric => {
                let num: u32 =
                    serde_json::from_value::<u32>(self.conf.variable_conf.identifier.clone())
                        .unwrap();
                Identifier::Numeric(num)
            }
            types::devices::opcua::IdentifierType::String => {
                let s: UAString =
                    serde_json::from_value(self.conf.variable_conf.identifier.clone()).unwrap();
                Identifier::String(s)
            }
            types::devices::opcua::IdentifierType::Guid => {
                let guid: Guid =
                    serde_json::from_value(self.conf.variable_conf.identifier.clone()).unwrap();
                Identifier::Guid(guid)
            }
            types::devices::opcua::IdentifierType::ByteString => {
                let bs: ByteString =
                    serde_json::from_value(self.conf.variable_conf.identifier.clone()).unwrap();
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
    ) -> HaliaResult<bool> {
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

        Ok(restart)
    }

    pub async fn delete(&self, device_id: &Uuid, group_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::opcua::delete_group_variable(device_id, group_id, &self.id).await?;
        Ok(())
    }

    // pub fn write(&mut self, data_value: Option<DataValue>) {
    //     match data_value {
    //         Some(data_value) => self.value = data_value.value,
    //         None => self.value = None,
    //     }
    // }

    // pub async fn start(&mut self, client: Arc<Session>) {
    //     if self.on {
    //         return;
    //     } else {
    //         self.on = true;
    //     }

    //     let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
    //     self.stop_signal_tx = Some(stop_signal_tx);
    //     self.event_loop(stop_signal_rx, client).await;
    // }

    // async fn event_loop(&mut self, mut stop_signal_rx: mpsc::Receiver<()>, client: Arc<Session>) {
    //     let interval = self.conf.variable_conf.interval;
    //     let value = self.value.clone();

    //     let namespace = self.conf.variable_conf.namespace;
    //     let identifier = match &self.conf.variable_conf.identifier_typ {
    //         types::devices::opcua::IdentifierType::Numeric => {
    //             let num: u32 =
    //                 serde_json::from_value::<u32>(self.conf.variable_conf.identifier.clone())
    //                     .unwrap();
    //             Identifier::Numeric(num)
    //         }
    //         types::devices::opcua::IdentifierType::String => {
    //             let s: UAString =
    //                 serde_json::from_value(self.conf.variable_conf.identifier.clone()).unwrap();
    //             Identifier::String(s)
    //         }
    //         types::devices::opcua::IdentifierType::Guid => {
    //             let guid: Guid =
    //                 serde_json::from_value(self.conf.variable_conf.identifier.clone()).unwrap();
    //             Identifier::Guid(guid)
    //         }
    //         types::devices::opcua::IdentifierType::ByteString => {
    //             let bs: ByteString =
    //                 serde_json::from_value(self.conf.variable_conf.identifier.clone()).unwrap();
    //             Identifier::ByteString(bs)
    //         }
    //     };
    //     let handle = tokio::spawn(async move {
    //         let mut interval = time::interval(Duration::from_millis(interval));

    //         let read_value_id = ReadValueId {
    //             node_id: NodeId {
    //                 namespace,
    //                 identifier,
    //             },
    //             attribute_id: 13,
    //             index_range: UAString::null(),
    //             data_encoding: QualifiedName::null(),
    //         };
    //         loop {
    //             select! {
    //                 _ = stop_signal_rx.recv() => {
    //                     return (stop_signal_rx, client)
    //                 }

    //                 _ = interval.tick() => {
    //                     Variable::read_variable(&client, &read_value_id, &value).await;
    //                 }
    //             }
    //         }
    //     });
    //     self.handle = Some(handle);
    // }

    async fn read_variable(
        client: &Arc<Session>,
        read_value_id: &ReadValueId,
        value: &Arc<RwLock<Option<Variant>>>,
    ) {
        match client
            .read(&[read_value_id.clone()], TimestampsToReturn::Both, 2000.0)
            .await
        {
            Ok(mut resp) => match resp.pop() {
                Some(data_value) => *(value.write().await) = data_value.value,
                None => {}
            },
            Err(e) => {
                debug!("err code :{:?}", e);
            }
        }
    }
}

use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use message::{Message, MessageBatch, MessageValue};
use opcua::{
    client::{DataChangeCallback, MonitoredItem, Session},
    types::{
        ByteString, DataValue, Guid, Identifier, MonitoredItemCreateRequest, NodeId, QualifiedName,
        ReadValueId, TimestampsToReturn, UAString,
    },
};
use tokio::{
    select,
    sync::{broadcast, watch, RwLock},
    task::JoinHandle,
    time,
};
use tracing::debug;
use types::devices::opcua::{GroupConf, SourceConf, Subscriptionconf, VariableConf};

use super::transfer_identifier;

pub struct Source {
    group_stop_signal_tx: watch::Sender<()>,

    // variables: Option<Arc<RwLock<(Vec<Variable>, Vec<ReadValueId>)>>>,
    group_join_handle: Option<
        JoinHandle<(
            Arc<RwLock<Option<Arc<Session>>>>,
            GroupConf,
            watch::Receiver<()>,
        )>,
    >,
    err: Option<String>,

    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Source {
    pub async fn new(opcua_client: Arc<RwLock<Option<Arc<Session>>>>, conf: SourceConf) -> Self {
        let (group_stop_signal_tx, group_stop_signal_rx) = watch::channel(());
        let (mb_tx, _) = broadcast::channel(16);
        let mut source = Self {
            group_stop_signal_tx,
            mb_tx: mb_tx.clone(),
            group_join_handle: None,
            err: None,
        };
        match conf.typ {
            types::devices::opcua::SourceType::Group => {
                let join_handle = Self::start_group(
                    group_stop_signal_rx,
                    conf.group.unwrap(),
                    opcua_client,
                    mb_tx.clone(),
                )
                .await;
                source.group_join_handle = Some(join_handle);
            }
            types::devices::opcua::SourceType::Subscription => {}
            types::devices::opcua::SourceType::MonitoredItem => todo!(),
        }

        source
    }

    pub fn validate_conf(conf: SourceConf) -> Result<()> {
        debug!("validate conf: {:?}", conf);
        match conf.typ {
            types::devices::opcua::SourceType::Group => match conf.group {
                Some(_group) => Ok(()),
                None => bail!("类型为组类型，配置为空"),
            },
            types::devices::opcua::SourceType::Subscription => match conf.subscription {
                Some(subscription) => {
                    if subscription.publishing_interval == 0 {
                        bail!("发布间隔必须大于0");
                    }
                    Ok(())
                }
                None => bail!("类型为订阅类型，配置为空"),
            },
            types::devices::opcua::SourceType::MonitoredItem => match conf.monitored_item {
                Some(_monitored_item) => Ok(()),
                None => bail!("类型为监控类型，配置为空"),
            },
        }
    }

    async fn start_group(
        mut stop_signal_rx: watch::Receiver<()>,
        conf: GroupConf,
        opcua_client: Arc<RwLock<Option<Arc<Session>>>>,
        mb_tx: broadcast::Sender<MessageBatch>,
    ) -> JoinHandle<(
        Arc<RwLock<Option<Arc<Session>>>>,
        GroupConf,
        watch::Receiver<()>,
    )> {
        let interval = conf.interval;
        let mut need_read_variable_fields = vec![];
        let mut need_read_variable_ids = vec![];
        for variable_conf in conf.variables.iter() {
            need_read_variable_fields.push(variable_conf.field.clone());
            need_read_variable_ids.push(Self::group_get_read_value_id(variable_conf));
        }

        let max_age = conf.max_age;

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));

            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        return (opcua_client, conf, stop_signal_rx);
                    }
                    _ = interval.tick() => {
                        Self::group_read_variables_from_remote(&opcua_client, &need_read_variable_fields, &need_read_variable_ids, max_age, &mb_tx).await;
                    }
                }
            }
        })
    }

    async fn start_subscription(
        conf: Subscriptionconf,
        opcua_client: Arc<Session>,
        mb_tx: broadcast::Sender<MessageBatch>,
    ) {
        let opcua_subscription_id = opcua_client
            .create_subscription(
                Duration::from_secs(conf.publishing_interval),
                conf.lifetime_count,
                conf.max_keep_alive_count,
                conf.max_notifications_per_publish,
                conf.priority,
                conf.publishing_enabled,
                DataChangeCallback::new(|dv, item| {
                    println!("Data change from server:");
                    Self::print_value(&dv, item);
                }),
            )
            .await
            .unwrap();

        // TODO
        let ns = 2;
        let items_to_create: Vec<MonitoredItemCreateRequest> = ["v1", "v2", "v3", "v4"]
            .iter()
            .map(|v| NodeId::new(ns, *v).into())
            .collect();

        let _ = opcua_client
            .create_monitored_items(
                opcua_subscription_id,
                TimestampsToReturn::Both,
                items_to_create,
            )
            .await
            .unwrap();
    }

    fn print_value(data_value: &DataValue, item: &MonitoredItem) {
        let node_id = &item.item_to_monitor().node_id;
        if let Some(ref value) = data_value.value {
            println!("Item \"{}\", Value = {:?}", node_id, value);
        } else {
            println!(
                "Item \"{}\", Value not found, error: {}",
                node_id,
                data_value.status.as_ref().unwrap()
            );
        }
    }

    async fn start_monitored_item(&mut self) {}

    pub async fn stop(&mut self) {
        self.group_stop_signal_tx.send(()).unwrap();
    }

    pub async fn update_conf(&mut self, old_conf: SourceConf, new_conf: SourceConf) {
        // if self.stop_signal_tx.is_some() && restart {
        //     self.stop_signal_tx
        //         .as_ref()
        //         .unwrap()
        //         .send(())
        //         .await
        //         .unwrap();

        //     let (stop_signal_rx, read_tx, device_err) =
        //         self.join_handle.take().unwrap().await.unwrap();
        //     // self.event_loop(self.ext_conf.interval, stop_signal_rx, read_tx, device_err)
        //     //     .await;
        // }
    }

    pub fn group_get_read_value_id(variable: &VariableConf) -> ReadValueId {
        let identifier = transfer_identifier(&variable.identifier);

        ReadValueId {
            node_id: NodeId {
                namespace: variable.namespace,
                identifier,
            },
            attribute_id: 13,
            index_range: UAString::null(),
            data_encoding: QualifiedName::null(),
        }
    }

    async fn group_read_variables_from_remote(
        opcua_client: &Arc<RwLock<Option<Arc<Session>>>>,
        need_read_variable_names: &Vec<String>,
        need_read_variable_ids: &Vec<ReadValueId>,
        max_age: f64,
        mb_tx: &broadcast::Sender<MessageBatch>,
    ) {
        match opcua_client.read().await.as_ref() {
            Some(client) => match client
                .read(
                    &need_read_variable_ids,
                    TimestampsToReturn::Neither,
                    max_age,
                )
                .await
            {
                Ok(data_values) => {
                    let mut message = Message::default();
                    // todo
                    for (index, data_value) in data_values.into_iter().enumerate() {
                        let name = unsafe { need_read_variable_names.get_unchecked(index) };
                        let value = match data_value.value {
                            Some(variant) => match variant {
                                opcua::types::Variant::Empty => MessageValue::Null,
                                opcua::types::Variant::Boolean(bool) => MessageValue::Boolean(bool),
                                opcua::types::Variant::SByte(i) => MessageValue::Int64(i as i64),
                                opcua::types::Variant::Byte(u) => MessageValue::Int64(u as i64),
                                opcua::types::Variant::Int16(i) => MessageValue::Int64(i as i64),
                                opcua::types::Variant::UInt16(u) => MessageValue::Int64(u as i64),
                                opcua::types::Variant::Int32(i) => MessageValue::Int64(i as i64),
                                opcua::types::Variant::UInt32(u) => MessageValue::Int64(u as i64),
                                opcua::types::Variant::Int64(i) => MessageValue::Int64(i as i64),
                                opcua::types::Variant::UInt64(u) => MessageValue::Int64(u as i64),
                                opcua::types::Variant::Float(f) => MessageValue::Float64(f as f64),
                                opcua::types::Variant::Double(f) => MessageValue::Float64(f),
                                // opcua::types::Variant::String(s) => MessageValue::String(s as String),
                                opcua::types::Variant::String(_s) => todo!(),
                                opcua::types::Variant::DateTime(_) => todo!(),
                                opcua::types::Variant::Guid(_) => todo!(),
                                opcua::types::Variant::StatusCode(_) => todo!(),
                                opcua::types::Variant::ByteString(_) => todo!(),
                                opcua::types::Variant::XmlElement(_) => todo!(),
                                opcua::types::Variant::QualifiedName(_) => todo!(),
                                opcua::types::Variant::LocalizedText(_) => todo!(),
                                opcua::types::Variant::NodeId(_) => todo!(),
                                opcua::types::Variant::ExpandedNodeId(_) => todo!(),
                                opcua::types::Variant::ExtensionObject(_) => todo!(),
                                opcua::types::Variant::Variant(_) => todo!(),
                                opcua::types::Variant::DataValue(_) => todo!(),
                                opcua::types::Variant::DiagnosticInfo(_) => todo!(),
                                opcua::types::Variant::Array(_) => todo!(),
                            },
                            None => MessageValue::Null,
                        };

                        message.add(name.clone(), value);
                    }
                    let mut mb = MessageBatch::default();
                    mb.push_message(message);
                    _ = mb_tx.send(mb);
                }
                Err(e) => {
                    debug!("err code :{:?}", e);
                    return;
                }
            },
            None => return,
        }
    }
}

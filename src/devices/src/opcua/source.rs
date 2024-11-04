use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use message::{Message, MessageBatch, MessageValue};
use opcua_protocol::{
    client::{DataChangeCallback, MonitoredItem, Session},
    types::{
        DataValue, ExtensionObject, MonitoredItemCreateRequest, MonitoringParameters,
        QualifiedName, ReadValueId, TimestampsToReturn, UAString,
    },
};
use tokio::{
    select,
    sync::{broadcast, watch, RwLock},
    task::JoinHandle,
    time,
};
use tracing::warn;
use types::devices::device::opcua::{GroupConf, SourceConf, Subscriptionconf, VariableConf};

use super::{transfer_monitoring_node, transfer_node_id};

pub struct Source {
    // variables: Option<Arc<RwLock<(Vec<Variable>, Vec<ReadValueId>)>>>,
    group_stop_signal_tx: Option<watch::Sender<()>>,
    group_join_handle: Option<JoinHandle<GroupJoinHandleData>>,
    err: Option<String>,

    pub mb_tx: broadcast::Sender<MessageBatch>,
}

pub struct GroupJoinHandleData {
    pub opcua_client: Arc<RwLock<Option<Arc<Session>>>>,
    pub conf: GroupConf,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Source {
    pub async fn new(opcua_client: Arc<RwLock<Option<Arc<Session>>>>, conf: SourceConf) -> Self {
        let (mb_tx, _) = broadcast::channel(16);

        let mut source = Self {
            group_stop_signal_tx: None,
            mb_tx: mb_tx.clone(),
            group_join_handle: None,
            err: None,
        };

        match conf.typ {
            types::devices::device::opcua::SourceType::Group => {
                let (group_stop_signal_tx, group_stop_signal_rx) = watch::channel(());
                let join_handle_data = GroupJoinHandleData {
                    opcua_client: opcua_client.clone(),
                    conf: conf.group.unwrap(),
                    stop_signal_rx: group_stop_signal_rx,
                    mb_tx,
                };
                let join_handle = Self::start_group(join_handle_data);
                source.group_stop_signal_tx = Some(group_stop_signal_tx);
                source.group_join_handle = Some(join_handle);
            }
            types::devices::device::opcua::SourceType::Subscription => {}
            types::devices::device::opcua::SourceType::MonitoredItem => todo!(),
        }

        source
    }

    pub fn validate_conf(conf: SourceConf) -> Result<()> {
        match conf.typ {
            types::devices::device::opcua::SourceType::Group => match conf.group {
                Some(_group) => Ok(()),
                None => bail!("类型为组类型，配置为空"),
            },
            types::devices::device::opcua::SourceType::Subscription => match conf.subscription {
                Some(subscription) => {
                    if subscription.publishing_interval == 0 {
                        bail!("发布间隔必须大于0");
                    }
                    Ok(())
                }
                None => bail!("类型为订阅类型，配置为空"),
            },
            types::devices::device::opcua::SourceType::MonitoredItem => match conf.monitored_item {
                Some(_monitored_item) => Ok(()),
                None => bail!("类型为监控类型，配置为空"),
            },
        }
    }

    fn start_group(mut join_handle_data: GroupJoinHandleData) -> JoinHandle<GroupJoinHandleData> {
        let mut need_read_variable_fields = vec![];
        let mut need_read_variable_ids = vec![];
        for variable_conf in join_handle_data.conf.variables.iter() {
            need_read_variable_fields.push(variable_conf.field.clone());
            need_read_variable_ids.push(Self::group_get_read_value_id(variable_conf));
        }

        tokio::spawn(async move {
            let mut interval =
                time::interval(Duration::from_millis(join_handle_data.conf.interval));

            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }
                    _ = interval.tick() => {
                        Self::group_read_variables_from_remote(&join_handle_data.opcua_client, &need_read_variable_fields, &need_read_variable_ids, join_handle_data.conf.max_age, &join_handle_data.mb_tx).await;
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
        let items_to_create: Vec<MonitoredItemCreateRequest> = conf
            .monitored_items
            .iter()
            .map(|v| MonitoredItemCreateRequest {
                item_to_monitor: ReadValueId {
                    node_id: transfer_node_id(&v.variable.node_id),
                    attribute_id: 13,
                    index_range: UAString::null(),
                    data_encoding: QualifiedName::null(),
                },
                monitoring_mode: transfer_monitoring_node(&v.monitoring_mode),
                requested_parameters: MonitoringParameters {
                    client_handle: v.client_handle,
                    sampling_interval: v.sampling_interval,
                    // TODO
                    filter: ExtensionObject::null(),
                    queue_size: v.queue_size,
                    discard_oldest: v.discard_oldest,
                },
            })
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
        match &self.group_stop_signal_tx {
            Some(stop_signal_tx) => stop_signal_tx.send(()).unwrap(),
            None => {}
        }
    }

    pub async fn update_conf(&mut self, _conf: SourceConf) {
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
        let node_id = transfer_node_id(&variable.node_id);

        ReadValueId {
            node_id,
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
        if mb_tx.receiver_count() == 0 {
            return;
        }
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
                    for (index, data_value) in data_values.into_iter().enumerate() {
                        let name = unsafe { need_read_variable_names.get_unchecked(index) };
                        let value = match data_value.value {
                            Some(variant) => match variant {
                                opcua_protocol::types::Variant::Empty => MessageValue::Null,
                                opcua_protocol::types::Variant::Boolean(bool) => {
                                    MessageValue::Boolean(bool)
                                }
                                opcua_protocol::types::Variant::SByte(i) => {
                                    MessageValue::Int64(i as i64)
                                }
                                opcua_protocol::types::Variant::Byte(u) => {
                                    MessageValue::Int64(u as i64)
                                }
                                opcua_protocol::types::Variant::Int16(i) => {
                                    MessageValue::Int64(i as i64)
                                }
                                opcua_protocol::types::Variant::UInt16(u) => {
                                    MessageValue::Int64(u as i64)
                                }
                                opcua_protocol::types::Variant::Int32(i) => {
                                    MessageValue::Int64(i as i64)
                                }
                                opcua_protocol::types::Variant::UInt32(u) => {
                                    MessageValue::Int64(u as i64)
                                }
                                opcua_protocol::types::Variant::Int64(i) => {
                                    MessageValue::Int64(i as i64)
                                }
                                opcua_protocol::types::Variant::UInt64(u) => {
                                    MessageValue::Int64(u as i64)
                                }
                                opcua_protocol::types::Variant::Float(f) => {
                                    MessageValue::Float64(f as f64)
                                }
                                opcua_protocol::types::Variant::Double(f) => {
                                    MessageValue::Float64(f)
                                }
                                opcua_protocol::types::Variant::String(s) => {
                                    MessageValue::String(s.to_string())
                                }
                                opcua_protocol::types::Variant::DateTime(_) => todo!(),
                                opcua_protocol::types::Variant::Guid(guid) => {
                                    MessageValue::String(guid.to_string())
                                }
                                opcua_protocol::types::Variant::StatusCode(s) => {
                                    MessageValue::String(s.name().to_owned())
                                }
                                opcua_protocol::types::Variant::ByteString(_) => todo!(),
                                opcua_protocol::types::Variant::XmlElement(_) => todo!(),
                                opcua_protocol::types::Variant::QualifiedName(_) => todo!(),
                                opcua_protocol::types::Variant::LocalizedText(_) => todo!(),
                                opcua_protocol::types::Variant::NodeId(_) => todo!(),
                                opcua_protocol::types::Variant::ExpandedNodeId(_) => todo!(),
                                opcua_protocol::types::Variant::ExtensionObject(_) => todo!(),
                                opcua_protocol::types::Variant::Variant(_) => todo!(),
                                opcua_protocol::types::Variant::DataValue(_) => todo!(),
                                opcua_protocol::types::Variant::DiagnosticInfo(_) => todo!(),
                                opcua_protocol::types::Variant::Array(_) => todo!(),
                            },
                            None => MessageValue::Null,
                        };

                        message.add(name.clone(), value);
                    }
                    let mut mb = MessageBatch::default();
                    mb.push_message(message);
                    mb_tx.send(mb).unwrap();
                }
                Err(e) => {
                    warn!("err code :{:?}", e);
                    return;
                }
            },
            None => return,
        }
    }
}

use std::{sync::Arc, time::Duration};

use anyhow::{bail, Result};
use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
};
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
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::debug;
use types::{
    devices::opcua::{GroupConf, SourceConf, VariableConf},
    BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,

    pub base_conf: BaseConf,
    pub ext_conf: SourceConf,

    on: bool,
    stop_signal_tx: Option<mpsc::Sender<()>>,

    // variables: Option<Arc<RwLock<(Vec<Variable>, Vec<ReadValueId>)>>>,
    group: Option<Group>,

    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            mpsc::Sender<Uuid>,
            Arc<RwLock<Option<String>>>,
        )>,
    >,
    err: Option<String>,

    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Source {
    pub fn new(source_id: Uuid, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<Self> {
        Self::validate_conf(&ext_conf)?;

        Ok(Self {
            id: source_id,
            base_conf,
            ext_conf,
            on: false,
            stop_signal_tx: None,
            group: None,
            mb_tx: None,
            join_handle: None,
            err: None,
        })
    }

    fn validate_conf(ext_conf: &SourceConf) -> Result<()> {
        match ext_conf.typ {
            types::devices::opcua::SourceType::Group => match &ext_conf.group {
                Some(group) => Ok(()),
                None => bail!("类型为组类型，配置为空"),
            },
            types::devices::opcua::SourceType::Subscription => match &ext_conf.subscription {
                Some(subscription) => {
                    if subscription.publishing_interval == 0 {
                        bail!("发布间隔必须大于0");
                    }
                    Ok(())
                }
                None => bail!("类型为订阅类型，配置为空"),
            },
            types::devices::opcua::SourceType::MonitoredItem => match &ext_conf.monitored_item {
                Some(monitored_item) => Ok(()),
                None => bail!("类型为监控类型，配置为空"),
            },
        }
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, _ext_conf: &SourceConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self)
    }

    pub async fn start(&mut self, opcua_client: Arc<Session>) {
        self.on = true;

        let (mb_tx, _) = broadcast::channel(16);

        match &self.ext_conf.typ {
            types::devices::opcua::SourceType::Group => {
                self.start_group(opcua_client, mb_tx.clone()).await
            }
            types::devices::opcua::SourceType::Subscription => {
                self.start_subscription(opcua_client, mb_tx.clone()).await
            }
            types::devices::opcua::SourceType::MonitoredItem => self.start_monitored_item().await,
        }

        self.mb_tx = Some(mb_tx);
    }

    async fn start_group(
        &mut self,
        opcua_client: Arc<Session>,
        mb_tx: broadcast::Sender<MessageBatch>,
    ) {
        let group_conf = self.ext_conf.group.as_ref().unwrap();
        let interval = group_conf.interval;
        let mut need_read_variable_names = vec![];
        let mut need_read_variable_ids = vec![];
        for variable_conf in group_conf.variables.iter() {
            need_read_variable_names.push(variable_conf.name.clone());
            need_read_variable_ids.push(Group::get_read_value_id(variable_conf));
        }

        let timestamp_to_return = match group_conf.timestamps_to_return {
            types::devices::opcua::TimestampsToReturn::Source => TimestampsToReturn::Source,
            types::devices::opcua::TimestampsToReturn::Server => TimestampsToReturn::Server,
            types::devices::opcua::TimestampsToReturn::Both => TimestampsToReturn::Both,
            types::devices::opcua::TimestampsToReturn::Neither => TimestampsToReturn::Neither,
            types::devices::opcua::TimestampsToReturn::Invalid => TimestampsToReturn::Invalid,
        };

        let max_age = group_conf.max_age as f64;

        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));

            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return;
                    }
                    _ = interval.tick() => {
                        Group::read_variables_from_remote(&opcua_client, &need_read_variable_names, &need_read_variable_ids, &timestamp_to_return, max_age, &mb_tx).await;
                    }
                }
            }
        });
    }

    async fn start_subscription(
        &mut self,
        opcua_client: Arc<Session>,
        mb_tx: broadcast::Sender<MessageBatch>,
    ) {
        let subscription_conf = self.ext_conf.subscription.as_ref().unwrap();
        let opcua_subscription_id = opcua_client
            .create_subscription(
                Duration::from_secs(subscription_conf.publishing_interval),
                subscription_conf.lifetime_count,
                subscription_conf.max_keep_alive_count,
                subscription_conf.max_notifications_per_publish,
                subscription_conf.priority,
                subscription_conf.publishing_enalbed,
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
        // self.stop_signal_tx
        //     .as_ref()
        //     .unwrap()
        //     .send(())
        //     .await
        //     .unwrap();
        // self.stop_signal_tx = None;
    }

    pub async fn update(&mut self, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<()> {
        Self::validate_conf(&ext_conf)?;

        self.base_conf = base_conf;

        if self.ext_conf == ext_conf {
            return Ok(());
        }
        self.ext_conf = ext_conf;

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

        Ok(())
    }
}

pub struct Group {
    variables: Arc<RwLock<Vec<(String, ReadValueId)>>>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
}

impl Group {
    pub fn new(conf: &GroupConf) -> Self {
        let mut variables = vec![];
        for variable_conf in conf.variables.iter() {
            let read_value_id = Self::get_read_value_id(variable_conf);
            variables.push((variable_conf.name.clone(), read_value_id));
        }
        Self {
            variables: Arc::new(RwLock::new(variables)),
            stop_signal_tx: None,
        }
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

    async fn read_variables_from_remote(
        opcua_client: &Arc<Session>,
        need_read_variable_names: &Vec<String>,
        need_read_variable_ids: &Vec<ReadValueId>,
        timestamp_to_return: &TimestampsToReturn,
        max_age: f64,
        mb_tx: &broadcast::Sender<MessageBatch>,
    ) {
        match opcua_client
            .read(
                &need_read_variable_ids,
                timestamp_to_return.clone(),
                max_age,
            )
            .await
        {
            Ok(data_values) => {
                let mut message = Message::default();
                // todo
                for (index, value) in data_values.into_iter().enumerate() {
                    let name = unsafe { need_read_variable_names.get_unchecked(index) };
                    let value = match value.value {
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
                            opcua::types::Variant::String(s) => todo!(),
                            opcua::types::Variant::DateTime(_) => todo!(),
                            opcua::types::Variant::Guid(_) => todo!(),
                            opcua::types::Variant::StatusCode(s) => todo!(),
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
            }
        }
    }
}

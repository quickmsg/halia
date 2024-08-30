use anyhow::Result;
use apps::App;
use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
};
use databoard::databoard::Databoard;
use devices::Device;
use functions::{computes, filter, merge::merge::Merge, window};
use message::MessageBatch;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error};
use types::rules::{
    functions::{ComputerConf, FilterConf, WindowConf},
    AppSinkNode, AppSourceNode, CreateUpdateRuleReq, DataboardNode, DeviceSinkNode,
    DeviceSourceNode, Node, NodeType, SearchRulesItemResp,
};
use uuid::Uuid;

use crate::segment::{get_3d_ids, start_segment, take_source_ids};

pub struct Rule {
    pub id: Uuid,
    pub conf: CreateUpdateRuleReq,
    pub on: bool,
    pub stop_signal_tx: Option<broadcast::Sender<()>>,
}

impl Rule {
    pub async fn new(
        devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
        apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
        databoards: &Arc<RwLock<Vec<Databoard>>>,
        rule_id: Uuid,
        req: CreateUpdateRuleReq,
    ) -> HaliaResult<Self> {
        let mut error = None;
        let mut add_ref_nodes = vec![];
        for node in req.ext.nodes.iter() {
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                    if let Err(e) = devices::add_source_ref(
                        devices,
                        &source_node.device_id,
                        &source_node.source_id,
                        &rule_id,
                    )
                    .await
                    {
                        add_ref_nodes.push(&node);
                        error = Some(format!("引用设备错误: {}", e).to_owned());
                        break;
                    }
                }
                NodeType::AppSource => {
                    let source_node: AppSourceNode = serde_json::from_value(node.conf.clone())?;
                    if let Err(e) = apps::add_source_ref(
                        apps,
                        &source_node.app_id,
                        &source_node.source_id,
                        &rule_id,
                    )
                    .await
                    {
                        add_ref_nodes.push(&node);
                        error = Some(format!("引用应用错误: {}", e).to_owned());
                        break;
                    }
                }
                NodeType::DeviceSink => {
                    let sink_node: DeviceSinkNode = serde_json::from_value(node.conf.clone())?;
                    if let Err(e) = devices::add_sink_ref(
                        devices,
                        &sink_node.device_id,
                        &sink_node.sink_id,
                        &rule_id,
                    )
                    .await
                    {
                        add_ref_nodes.push(&node);
                        error = Some(format!("引用设备错误: {}", e).to_owned());
                        break;
                    }
                }
                NodeType::AppSink => {
                    let sink_node: AppSinkNode = serde_json::from_value(node.conf.clone())?;
                    if let Err(e) =
                        apps::add_sink_ref(apps, &sink_node.app_id, &sink_node.sink_id, &rule_id)
                            .await
                    {
                        add_ref_nodes.push(&node);
                        error = Some(format!("引用应用错误: {}", e).to_owned());
                        break;
                    }
                }
                NodeType::Databoard => {
                    let databoard_node: DataboardNode = serde_json::from_value(node.conf.clone())?;
                    if let Err(e) = databoard::add_data_ref(
                        databoards,
                        &databoard_node.databoard_id,
                        &databoard_node.data_id,
                        &rule_id,
                    )
                    .await
                    {
                        add_ref_nodes.push(&node);
                        error = Some(format!("引用数据看板错误: {}", e).to_owned());
                        break;
                    }
                }
                _ => {}
            }
        }

        if let Some(e) = error {
            // TODO 回滚
            return Err(HaliaError::Common(e));
        }

        Ok(Self {
            on: false,
            id: rule_id,
            conf: req,
            stop_signal_tx: None,
        })
    }

    pub fn search(&self) -> SearchRulesItemResp {
        SearchRulesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            on: self.on,
        }
    }

    pub async fn start(
        &mut self,
        devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
        apps: &Arc<RwLock<Vec<Box<dyn App>>>>,
    ) -> Result<()> {
        check_and_set_on_true!(self);

        let (stop_signal_tx, _) = broadcast::channel(16);

        let (incoming_edges, outgoing_edges) = self.conf.get_edges();
        let mut tmp_incoming_edges = incoming_edges.clone();
        let mut tmp_outgoing_edges = outgoing_edges.clone();

        let mut node_map = HashMap::<usize, Node>::new();
        for node in self.conf.ext.nodes.iter() {
            node_map.insert(node.index, node.clone());
        }

        let mut ids: Vec<usize> = self.conf.ext.nodes.iter().map(|node| node.index).collect();

        let mut receivers = HashMap::new();

        let source_ids =
            take_source_ids(&mut ids, &mut tmp_incoming_edges, &mut tmp_outgoing_edges);

        for source_id in source_ids {
            let node = node_map.get(&source_id).unwrap();
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: DeviceSourceNode = serde_json::from_value(node.conf.clone())?;
                    let cnt = tmp_outgoing_edges.get(&source_id).unwrap().len();
                    let mut rxs = vec![];
                    for _ in 0..cnt {
                        rxs.push(
                            match devices::get_source_rx(
                                devices,
                                &source_node.device_id,
                                &source_node.source_id,
                                &self.id,
                            )
                            .await
                            {
                                Ok(rx) => rx,
                                Err(e) => return Err(e.into()),
                            },
                        )
                    }
                    receivers.insert(source_id, rxs);
                }
                NodeType::AppSource => {
                    let source_node: AppSourceNode = serde_json::from_value(node.conf.clone())?;
                    let cnt = tmp_outgoing_edges.get(&source_id).unwrap().len();
                    let mut rxs = vec![];
                    for _ in 0..cnt {
                        debug!("here");
                        rxs.push(
                            match apps::get_source_rx(
                                apps,
                                &source_node.app_id,
                                &source_node.source_id,
                                &self.id,
                            )
                            .await
                            {
                                Ok(rx) => rx,
                                Err(e) => return Err(e.into()),
                            },
                        )
                    }
                    receivers.insert(source_id, rxs);
                }
                _ => unreachable!(),
            }
        }

        let threed_ids = get_3d_ids(
            &mut ids,
            &node_map,
            &mut tmp_incoming_edges,
            &mut tmp_outgoing_edges,
        )?;

        for twod_ids in threed_ids {
            for oned_ids in twod_ids {
                let mut functions = vec![];
                let mut ids = vec![];
                let mut mpsc_tx: Option<mpsc::Sender<MessageBatch>> = None;
                let mut broadcast_tx: Option<broadcast::Sender<MessageBatch>> = None;

                for id in oned_ids {
                    let node = node_map.get(&id).unwrap();
                    match node.node_type {
                        NodeType::Merge => {
                            let source_ids = incoming_edges.get(&id).unwrap();
                            let mut rxs = vec![];
                            for source_id in source_ids {
                                rxs.push(receivers.get_mut(source_id).unwrap().pop().unwrap());
                            }
                            let (tx, _) = broadcast::channel::<MessageBatch>(16);
                            let mut n_rxs = vec![];
                            let cnt = outgoing_edges.get(&id).unwrap().len();
                            for _ in 0..cnt {
                                n_rxs.push(tx.subscribe());
                            }
                            receivers.insert(id, n_rxs);
                            match Merge::new(rxs, tx) {
                                Ok(mut merge) => {
                                    merge.run().await;
                                }
                                Err(e) => error!("create merge err:{}", e),
                            }
                            break;
                        }
                        NodeType::Window => {
                            let source_ids = incoming_edges.get(&id).unwrap();
                            let source_id = source_ids[0];
                            let rx = receivers.get_mut(&source_id).unwrap().pop().unwrap();
                            let (tx, _) = broadcast::channel::<MessageBatch>(16);
                            let mut rxs = vec![];
                            let cnt = outgoing_edges.get(&id).unwrap().len();
                            for _ in 0..cnt {
                                rxs.push(tx.subscribe());
                            }
                            receivers.insert(id, rxs);
                            let window_conf: WindowConf =
                                serde_json::from_value(node.conf.clone())?;
                            window::run(window_conf, rx, tx, stop_signal_tx.subscribe()).unwrap();
                        }
                        NodeType::Filter => {
                            let conf: FilterConf = serde_json::from_value(node.conf.clone())?;
                            functions.push(filter::new(conf)?);
                            ids.push(id);
                        }
                        NodeType::Computer => {
                            let conf: ComputerConf = serde_json::from_value(node.conf.clone())?;
                            functions.push(computes::new(conf)?);
                            ids.push(id);
                        }
                        NodeType::DeviceSink => {
                            ids.push(id);
                            let sink_node: DeviceSinkNode =
                                serde_json::from_value(node.conf.clone())?;
                            let tx = match devices::get_sink_tx(
                                devices,
                                &sink_node.device_id,
                                &sink_node.sink_id,
                                &self.id,
                            )
                            .await
                            {
                                Ok(tx) => tx,
                                Err(e) => return Err(e.into()),
                            };
                            mpsc_tx = Some(tx);
                        }
                        NodeType::AppSink => {
                            ids.push(id);
                            let sink_node: AppSinkNode = serde_json::from_value(node.conf.clone())?;
                            let tx = match apps::get_sink_tx(
                                apps,
                                &sink_node.app_id,
                                &sink_node.sink_id,
                                &self.id,
                            )
                            .await
                            {
                                Ok(tx) => tx,
                                Err(e) => return Err(e.into()),
                            };
                            mpsc_tx = Some(tx);
                        }
                        _ => {}
                    }
                }

                if ids.len() > 0 {
                    let source_ids = incoming_edges.get(&ids[0]).unwrap();
                    let source_id = source_ids[0];
                    let rx = receivers.get_mut(&source_id).unwrap().pop().unwrap();
                    if mpsc_tx.is_none() {
                        let (tx, _) = broadcast::channel::<MessageBatch>(16);
                        let mut rxs = vec![];
                        let cnt = outgoing_edges.get(&ids.last().unwrap()).unwrap().len();
                        for _ in 0..cnt {
                            rxs.push(tx.subscribe());
                        }
                        receivers.insert(*ids.last().unwrap(), rxs);
                        broadcast_tx = Some(tx);
                    }
                    // TODO
                    start_segment(
                        rx,
                        functions,
                        mpsc_tx,
                        broadcast_tx,
                        stop_signal_tx.subscribe(),
                    );
                }
            }
        }

        self.stop_signal_tx = Some(stop_signal_tx);
        Ok(())
    }

    pub fn stop(&mut self) -> HaliaResult<()> {
        check_and_set_on_false!(self);

        if let Err(e) = self.stop_signal_tx.as_ref().unwrap().send(()) {
            error!("rule stop send signal err:{}", e);
        }

        Ok(())
    }
}

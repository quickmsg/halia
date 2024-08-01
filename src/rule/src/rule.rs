use anyhow::Result;
use apps::mqtt_client::manager::GLOBAL_MQTT_CLIENT_MANAGER;
use common::{error::HaliaResult, persistence};
use devices::modbus::manager::GLOBAL_MODBUS_MANAGER;
use functions::{filter, merge::merge::Merge, window};
use message::MessageBatch;
use std::collections::HashMap;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error};
use types::rules::{
    apps::mqtt_client,
    devices::modbus,
    functions::{FilterConf, WindowConf},
    CreateUpdateRuleReq, Node, NodeType, SearchRulesItemResp, SinkNode, SourceNode,
};
use uuid::Uuid;

use crate::segment::{get_3d_ids, start_segment, take_source_ids};

pub struct Rule {
    pub id: Uuid,
    pub conf: CreateUpdateRuleReq,
    pub stop_signal_tx: Option<broadcast::Sender<()>>,
}

impl Rule {
    pub async fn new(id: Option<Uuid>, req: CreateUpdateRuleReq) -> HaliaResult<Self> {
        let (id, new) = match id {
            Some(id) => (id, false),
            None => (Uuid::new_v4(), true),
        };

        for node in req.ext.nodes.iter() {
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: SourceNode = serde_json::from_value(node.conf.clone())?;
                    match source_node.typ.as_str() {
                        devices::modbus::TYPE => {
                            let source: modbus::SourcePoint =
                                serde_json::from_value(source_node.conf.clone())?;
                            if let Err(e) = GLOBAL_MODBUS_MANAGER
                                .add_point_ref(&source.device_id, &source.point_id, &id)
                                .await
                            {
                                todo!()
                            }
                        }
                        devices::opcua::TYPE => {}
                        devices::coap::TYPE => {}
                        _ => unreachable!(),
                    }
                }
                NodeType::AppSource => {
                    let source_node: SourceNode = serde_json::from_value(node.conf.clone())?;
                    match source_node.typ.as_str() {
                        apps::mqtt_client::TYPE => {
                            let source: mqtt_client::Source =
                                serde_json::from_value(source_node.conf.clone())?;
                        }
                        _ => unreachable!(),
                    }
                }
                NodeType::DeviceSink => {
                    let sink_node: SinkNode = serde_json::from_value(node.conf.clone())?;
                    match sink_node.typ.as_str() {
                        devices::modbus::TYPE => {}
                        devices::opcua::TYPE => {}
                        devices::coap::TYPE => {}
                        _ => unreachable!(),
                    }
                }
                NodeType::AppSink => {
                    let sink_node: SinkNode = serde_json::from_value(node.conf.clone())?;
                    match sink_node.typ.as_str() {
                        apps::mqtt_client::TYPE => {
                            let sink: mqtt_client::Sink =
                                serde_json::from_value(sink_node.conf.clone())?;
                            match GLOBAL_MQTT_CLIENT_MANAGER.add_sink_ref(
                                &sink.app_id,
                                &sink.sink_id,
                                &id,
                            ) {
                                Ok(_) => {}
                                Err(_) => todo!(),
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                _ => {}
            }
        }

        if new {
            persistence::rule::create(&id, serde_json::to_string(&req).unwrap()).await?;
        }

        Ok(Self {
            id,
            conf: req,
            stop_signal_tx: None,
        })
    }

    pub fn search(&self) -> SearchRulesItemResp {
        SearchRulesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
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
        debug!("source ids {:?}", source_ids);

        for source_id in source_ids {
            let node = node_map.get(&source_id).unwrap();
            match node.node_type {
                NodeType::DeviceSource => {
                    let source_node: SourceNode = serde_json::from_value(node.conf.clone())?;
                    let rxs = match source_node.typ.as_str() {
                        devices::modbus::TYPE => {
                            let source_point: modbus::SourcePoint =
                                serde_json::from_value(source_node.conf.clone())?;

                            let cnt = tmp_outgoing_edges.get(&source_id).unwrap().len();
                            let mut rxs = vec![];
                            for _ in 0..cnt {
                                rxs.push(
                                    GLOBAL_MODBUS_MANAGER
                                        .get_point_mb_rx(
                                            &source_point.device_id,
                                            &source_point.point_id,
                                            &self.id,
                                        )
                                        .await
                                        .unwrap(),
                                )
                            }
                            rxs
                        }
                        _ => unreachable!(),
                    };
                    receivers.insert(source_id, rxs);
                }
                NodeType::AppSource => {
                    let source_node: SourceNode = serde_json::from_value(node.conf.clone())?;
                    let rxs = match source_node.typ.as_str() {
                        apps::mqtt_client::TYPE => {
                            let source: mqtt_client::Source =
                                serde_json::from_value(source_node.conf.clone())?;

                            let cnt = tmp_outgoing_edges.get(&source_id).unwrap().len();
                            let mut rxs = vec![];
                            for _ in 0..cnt {
                                rxs.push(
                                    GLOBAL_MQTT_CLIENT_MANAGER
                                        .get_source_mb_rx(
                                            &source.app_id,
                                            &source.source_id,
                                            &self.id,
                                        )
                                        .await
                                        .unwrap(),
                                )
                            }
                            rxs
                        }
                        _ => {
                            todo!()
                        }
                    };
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

        debug!("{:?}", threed_ids);

        for twod_ids in threed_ids {
            for oned_ids in twod_ids {
                debug!("{:?}", oned_ids);
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
                            let conf: Vec<FilterConf> = serde_json::from_value(node.conf.clone())?;
                            functions.push(filter::new(conf)?);
                            ids.push(id);
                        }
                        NodeType::DeviceSink => {
                            let sink_node: SinkNode = serde_json::from_value(node.conf.clone())?;
                            let tx = match sink_node.typ.as_str() {
                                devices::modbus::TYPE => {
                                    let sink: types::rules::devices::modbus::Sink =
                                        serde_json::from_value(sink_node.conf.clone())?;
                                    GLOBAL_MODBUS_MANAGER
                                        .get_sink_mb_tx(&sink.device_id, &sink.sink_id, &self.id)
                                        .await
                                        .unwrap()
                                }
                                _ => todo!(),
                            };
                            mpsc_tx = Some(tx);
                        }
                        NodeType::AppSink => {
                            ids.push(id);
                            let sink_node: SinkNode = serde_json::from_value(node.conf.clone())?;
                            let tx = match sink_node.typ.as_str() {
                                apps::mqtt_client::TYPE => {
                                    let sink: mqtt_client::Sink =
                                        serde_json::from_value(sink_node.conf.clone())?;
                                    GLOBAL_MQTT_CLIENT_MANAGER
                                        .get_sink_mb_tx(&sink.app_id, &sink.sink_id, &self.id)
                                        .await
                                        .unwrap()
                                }
                                _ => {
                                    todo!()
                                }
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

    pub fn stop(&mut self) {
        if let Err(e) = self.stop_signal_tx.as_ref().unwrap().send(()) {
            error!("rule stop send signal err:{}", e);
        }
    }
}

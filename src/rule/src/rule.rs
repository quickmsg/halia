use anyhow::Result;
use apps::mqtt_client::manager::GLOBAL_MQTT_CLIENT_MANAGER;
use common::{error::HaliaResult, persistence};
use devices::modbus::manager::GLOBAL_MODBUS_MANAGER;
use functions::{merge::merge::Merge, window};
use message::MessageBatch;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tracing::{debug, error};
use types::rules::{
    apps::mqtt_client, devices::modbus, functions::WindowConf, CreateUpdateRuleReq, Node, NodeType,
    SearchRulesItemResp, SinkNode, SourceNode,
};
use uuid::Uuid;

use crate::segment::{get_segments, take_source_ids};

pub struct Rule {
    pub id: Uuid,
    pub conf: CreateUpdateRuleReq,
    pub stop_signal_rx: Option<broadcast::Sender<()>>,
}

impl Rule {
    pub async fn new(id: Option<Uuid>, req: CreateUpdateRuleReq) -> HaliaResult<Self> {
        let (id, new) = match id {
            Some(id) => (id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::rule::create(&id, serde_json::to_string(&req).unwrap()).await?;
        }

        let (incoming_edges, outgoing_edges) = req.get_edges();
        let mut tmp_incoming_edges = incoming_edges.clone();
        let mut tmp_outgoing_edges = outgoing_edges.clone();

        let mut node_map = HashMap::<usize, Node>::new();
        for node in req.nodes.iter() {
            node_map.insert(node.index, node.clone());
        }

        let mut ids: Vec<usize> = req.nodes.iter().map(|node| node.index).collect();
        let source_ids =
            take_source_ids(&mut ids, &mut tmp_incoming_edges, &mut tmp_outgoing_edges);
        debug!("source ids {:?}", source_ids);
        debug!("{:?}", ids);
        let segments = get_segments(
            &mut ids,
            &node_map,
            &mut tmp_incoming_edges,
            &mut tmp_outgoing_edges,
        );

        debug!("{:?}", segments);

        Ok(Self {
            id,
            conf: req,
            stop_signal_rx: None,
        })
    }

    pub fn search(&self) -> SearchRulesItemResp {
        SearchRulesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (incoming_edges, outgoing_edges) = self.conf.get_edges();
        let mut tmp_incoming_edges = incoming_edges.clone();
        let mut tmp_outgoing_edges = outgoing_edges.clone();

        let mut node_map = HashMap::<usize, Node>::new();
        for node in self.conf.nodes.iter() {
            node_map.insert(node.index, node.clone());
        }

        let mut ids: Vec<usize> = self.conf.nodes.iter().map(|node| node.index).collect();

        let source_ids =
            take_source_ids(&mut ids, &mut tmp_incoming_edges, &mut tmp_outgoing_edges);
        debug!("source ids {:?}", source_ids);

        let all_segments = get_segments(
            &mut ids,
            &node_map,
            &mut tmp_incoming_edges,
            &mut tmp_outgoing_edges,
        )?;

        debug!("{:?}", all_segments);

        let mut receivers = HashMap::new();

        for segments in all_segments {
            for segment in segments {
                for (i, id) in segment.ids.iter().enumerate() {
                    let node = node_map.get(&id).unwrap();
                    match node.node_type {
                        NodeType::DeviceSource => {
                            let source_node: SourceNode =
                                serde_json::from_value(node.conf.clone())?;
                            let rx = match source_node.r#type.as_str() {
                                devices::modbus::TYPE => {
                                    let source: modbus::Source =
                                        serde_json::from_value(source_node.conf.clone())?;
                                    GLOBAL_MODBUS_MANAGER
                                        .subscribe(
                                            &source.device_id,
                                            &source.source_id,
                                            &Uuid::new_v4(),
                                        )
                                        .await
                                        .unwrap()
                                }
                                _ => unreachable!(),
                            };
                            receivers.insert(*id, rx);
                            break;
                        }
                        NodeType::AppSource => {
                            let source_node: SourceNode =
                                serde_json::from_value(node.conf.clone())?;
                            let rx = match source_node.r#type.as_str() {
                                apps::mqtt_client::TYPE => {
                                    let source: mqtt_client::Source =
                                        serde_json::from_value(source_node.conf.clone())?;
                                    GLOBAL_MQTT_CLIENT_MANAGER
                                        .subscribe(&source.app_id, &source.source_id)
                                        .await
                                        .unwrap()
                                }
                                _ => {
                                    todo!()
                                }
                            };
                            receivers.insert(*id, rx);
                            break;
                        }
                        NodeType::Merge => {
                            let source_ids = incoming_edges.get(&id).unwrap();
                            let mut rxs = vec![];
                            for source_id in source_ids {
                                rxs.push(receivers.remove(source_id).unwrap());
                            }
                            let (tx, nrx) = broadcast::channel::<MessageBatch>(16);
                            receivers.insert(*id, nrx);
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
                            let rx = receivers.remove(&source_id).unwrap();
                            let (tx, nrx) = broadcast::channel::<MessageBatch>(16);
                            receivers.insert(*id, nrx);
                            let window_conf: WindowConf =
                                serde_json::from_value(node.conf.clone())?;
                            window::run(
                                window_conf,
                                rx,
                                tx,
                                self.stop_signal_rx.as_ref().unwrap().subscribe(),
                            )
                            .unwrap();
                            break;
                        }
                        NodeType::filter => {
                            todo!()
                        }
                        NodeType::DeviceSink => {
                            // if let Some(source_ids) = incoming_edges.get(&segment.first_id) {
                            //     if let Some(source_id) = source_ids.first() {
                            //         if let Some(mut node_receivers) = receivers.remove(source_id) {
                            //             let mut rx = node_receivers.remove(0);

                            //             let node = node_map.get(&info.id).unwrap();
                            //             let sink_node: SinkNode =
                            //                 serde_json::from_value(node.conf.clone())?;

                            //             let tx = match sink_node.r#type.as_str() {
                            //                 devices::modbus::TYPE => {
                            //                     let sink: types::rules::devices::modbus::Sink =
                            //                         serde_json::from_value(sink_node.conf.clone())?;
                            //                     GLOBAL_MODBUS_MANAGER
                            //                         .publish(
                            //                             &sink.device_id,
                            //                             &sink.sink_id,
                            //                             Uuid::new_v4().as_ref(),
                            //                         )
                            //                         .await
                            //                         .unwrap()
                            //                 }
                            //                 _ => todo!(),
                            //             };

                            //             tokio::spawn(async move {
                            //                 loop {
                            //                     match rx.recv().await {
                            //                         Ok(mb) => {
                            //                             let _ = tx.send(mb).await;
                            //                         }
                            //                         Err(_) => {
                            //                             debug!("recv err");
                            //                             return;
                            //                         }
                            //                     };
                            //                 }
                            //             });
                            //         }
                            //     }
                            // }
                        }
                        NodeType::AppSink => {
                            // if let Some(source_ids) = incoming_edges.get(&info.id) {
                            //     if let Some(source_id) = source_ids.first() {
                            //         if let Some(mut node_receivers) = receivers.remove(source_id) {
                            //             let mut rx = node_receivers.remove(0);

                            //             let node = node_map.get(&info.id).unwrap();
                            //             let sink_node: SinkNode =
                            //                 serde_json::from_value(node.conf.clone())?;

                            //             let tx = match sink_node.r#type.as_str() {
                            //                 apps::mqtt_client::TYPE => {
                            //                     let sink: mqtt_client::Sink =
                            //                         serde_json::from_value(sink_node.conf.clone())?;
                            //                     GLOBAL_MQTT_CLIENT_MANAGER
                            //                         .publish(&sink.app_id, &sink.sink_id)
                            //                         .await
                            //                         .unwrap()
                            //                 }
                            //                 _ => {
                            //                     todo!()
                            //                 }
                            //             };

                            //             tokio::spawn(async move {
                            //                 // TODO select stop signal
                            //                 loop {
                            //                     match rx.recv().await {
                            //                         Ok(mb) => {
                            //                             let _ = tx.send(mb).await;
                            //                         }
                            //                         Err(_) => {
                            //                             debug!("recv err");
                            //                             return;
                            //                         }
                            //                     };
                            //                 }
                            //             });
                            //         }
                            //     }
                            // }
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    pub fn stop(&mut self) {
        if let Err(e) = self.stop_signal_rx.as_ref().unwrap().send(()) {
            error!("rule stop send signal err:{}", e);
        }
    }
}

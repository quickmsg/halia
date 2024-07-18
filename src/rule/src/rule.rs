use anyhow::Result;
use apps::mqtt_client::manager::GLOBAL_MQTT_CLIENT_MANAGER;
use common::{error::HaliaResult, persistence};
use std::collections::HashMap;
use tokio::sync::broadcast;
use tracing::{debug, error};
use types::rules::{
    apps::mqtt_client, CreateUpdateRuleReq, Node, NodeType, SearchRulesItemResp, SinkNode,
    SourceNode,
};
use uuid::Uuid;

// use crate::stream::start_stream;

pub(crate) struct Rule {
    pub id: Uuid,
    pub conf: CreateUpdateRuleReq,
    pub stop_signal: Option<broadcast::Sender<()>>,
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

        Ok(Self {
            id,
            conf: req,
            stop_signal: None,
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

        let mut receivers = HashMap::new();
        let stream_infos = get_stream_infos(
            &mut ids,
            &node_map,
            &mut tmp_incoming_edges,
            &mut tmp_outgoing_edges,
        )?;

        for stream_info in stream_infos {
            for info in stream_info {
                match info.r#type {
                    NodeType::DeviceSource => {
                        let node = node_map.get(&info.first_id).unwrap();
                        let source_node: SourceNode = serde_json::from_value(node.conf.clone())?;
                        match source_node.r#type.as_str() {
                            "xxx" => {}
                            _ => {}
                        }
                        // let receiver = match source.r#type {
                        //     types::rule::CreateRuleSourceType::Device(r#type) => {
                        //         match r#type.as_str() {
                        //             modbus::TYPE => GLOBAL_MODBUS_MANAGER
                        //                 .subscribe(&source.id, &source.source_id.unwrap())
                        //                 .await
                        //                 .unwrap(),
                        //             _ => unreachable!(),
                        //         }
                        //     }
                        //     // types::rule::CreateRuleSourceType::App => GLOBAL_APP_MANAGER
                        //     //     .subscribe(&source.id, source.source_id)
                        //     //     .await
                        //     //     .unwrap(),
                        //     _ => todo!(),
                        // };
                        // receivers.insert(info.first_id, vec![receiver]);
                    }
                    NodeType::AppSource => {
                        let node = node_map.get(&info.first_id).unwrap();
                        let source_node: SourceNode = serde_json::from_value(node.conf.clone())?;
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
                        receivers.insert(info.first_id, vec![rx]);
                    }
                    NodeType::Window => {
                        // "window" => {
                        //     if let Some(source_ids) = incoming_edges.get(&info.id) {
                        //         if let Some(source_id) = source_ids.first() {
                        //             if let Some(mut node_receivers) = receivers.remove(source_id) {
                        //                 let rx = node_receivers.remove(0);
                        //                 let (tx, nrx) = broadcast::channel::<MessageBatch>(10);
                        //                 receivers.insert(info.id, vec![nrx]);

                        //                 if let Some(node) = node_map.get(&info.id) {
                        //                     let mut window =
                        //                         Window::new(node.conf.clone(), rx, tx)?;
                        //                     tokio::spawn(async move {
                        //                         window.run().await;
                        //                     });
                        //                 }
                        //             }
                        //         }
                        //     }
                        // }
                    }
                    NodeType::Merge => {
                        // if let Some(source_ids) = incoming_edges.get(&osi.id) {
                        //     debug!("merge source_ids:{:?}, ois.id:{}", source_ids, &osi.id);
                        //     let mut rxs = Vec::new();
                        //     for source_id in source_ids {
                        //         if let Some(mut node_receivers) = receivers.remove(source_id) {
                        //             let rx = node_receivers.remove(0);
                        //             rxs.push(rx);
                        //         }
                        //     }
                        //     debug!("source receivers len:{}", rxs.len());
                        //     let (tx, nrx) = broadcast::channel::<MessageBatch>(10);
                        //     receivers.insert(osi.id, vec![nrx]);
                        //     if let Some(node) = node_map.get(&osi.id) {
                        //         match Merge::new(node.conf.clone(), rxs, tx) {
                        //             Ok(mut merge) => {
                        //                 tokio::spawn(async move {
                        //                     merge.run().await;
                        //                 });
                        //             }
                        //             Err(e) => error!("create merge err:{}", e),
                        //         }
                        //     }
                        // }
                    }

                    // NodeType::DeviceSink => {
                    //     if let Some(source_ids) = incoming_edges.get(&info.id) {
                    //         if let Some(source_id) = source_ids.first() {
                    //             if let Some(mut node_receivers) = receivers.remove(source_id) {
                    //                 let mut rx = node_receivers.remove(0);

                    //                 let node = node_map.get(&info.id).unwrap();
                    //                 let sink: CreateRuleSink =
                    //                     serde_json::from_value(node.conf.clone())?;
                    //                 let tx = match sink.r#type {
                    //                     CreateRuleSinkType::Device(r#type) => match r#type.as_str()
                    //                     {
                    //                         modbus::TYPE => GLOBAL_MODBUS_MANAGER
                    //                             .publish(&sink.id, &sink.sink_id.unwrap())
                    //                             .await
                    //                             .unwrap(),
                    //                         _ => todo!(),
                    //                     },

                    //                     // CreateRuleSinkType::App => GLOBAL_APP_MANAGER
                    //                     //     .publish(&sink.id, &sink.sink_id)
                    //                     //     .await
                    //                     //     .unwrap(),
                    //                     _ => todo!(),
                    //                 };

                    //                 tokio::spawn(async move {
                    //                     loop {
                    //                         match rx.recv().await {
                    //                             Ok(mb) => {
                    //                                 let _ = tx.send(mb).await;
                    //                             }
                    //                             Err(_) => {
                    //                                 debug!("recv err");
                    //                                 return;
                    //                             }
                    //                         };
                    //                     }
                    //                 });
                    //             }
                    //         }
                    //     }
                    // }
                    NodeType::AppSink => {
                        if let Some(source_ids) = incoming_edges.get(&info.id) {
                            if let Some(source_id) = source_ids.first() {
                                if let Some(mut node_receivers) = receivers.remove(source_id) {
                                    let mut rx = node_receivers.remove(0);

                                    let node = node_map.get(&info.id).unwrap();
                                    let sink_node: SinkNode =
                                        serde_json::from_value(node.conf.clone())?;

                                    let tx = match sink_node.r#type.as_str() {
                                        apps::mqtt_client::TYPE => {
                                            let sink: mqtt_client::Sink =
                                                serde_json::from_value(sink_node.conf.clone())?;
                                            GLOBAL_MQTT_CLIENT_MANAGER
                                                .publish(&sink.app_id, &sink.sink_id)
                                                .await
                                                .unwrap()
                                        }
                                        _ => {
                                            todo!()
                                        }
                                    };

                                    tokio::spawn(async move {
                                        // TODO select stop signal
                                        loop {
                                            match rx.recv().await {
                                                Ok(mb) => {
                                                    let _ = tx.send(mb).await;
                                                }
                                                Err(_) => {
                                                    debug!("recv err");
                                                    return;
                                                }
                                            };
                                        }
                                    });
                                }
                            }
                        }
                    }

                    // "join" => {
                    //     if let Some(source_ids) = incoming_edges.get(&osi.id) {
                    //         let mut rxs = Vec::new();
                    //         for source_id in source_ids {
                    //             if let Some(mut node_receivers) = receivers.remove(source_id) {
                    //                 let rx = node_receivers.remove(0);
                    //                 rxs.push(rx);
                    //             }
                    //         }
                    //         let (tx, nrx) = broadcast::channel::<Message>(10);
                    //         receivers.insert(osi.id, vec![nrx]);
                    //         let mut join = Join::new(None, rxs, tx)?;
                    //         tokio::spawn(async move {
                    //             join.run().await;
                    //         });
                    //     }
                    // }
                    _ => {
                        debug!("{:?}", info.r#type);
                    }
                }
                // None => {
                //     if let Some(source_ids) = incoming_edges.get(&info.first_id) {
                //         if let Some(source_id) = source_ids.first() {
                //             if let Some(mut node_receivers) = receivers.remove(source_id) {
                //                 let rx = node_receivers.remove(0);
                //                 let (tx, nrx) = broadcast::channel::<MessageBatch>(10);
                //                 receivers.insert(info.last_id, vec![nrx]);

                //                 let mut nodes = Vec::new();
                //                 for id in info.ids.iter() {
                //                     nodes.push(node_map.get(id).unwrap());
                //                 }

                //                 debug!("create_graph_nodes:{:?}", nodes);
                //                 start_stream(nodes, rx, tx, self.stop_signal.subscribe()).await;
                //             }
                //         }
                //     }
                // }
            }
        }

        Ok(())
    }

    pub fn stop(&mut self) {
        if let Err(e) = self.stop_signal.as_ref().unwrap().send(()) {
            error!("rule stop send signal err:{}", e);
        }
    }
}

#[derive(Debug)]
struct StreamInfo {
    r#type: NodeType,
    id: usize,
    first_id: usize,
    last_id: usize,
    ids: Vec<usize>,
}

fn get_stream_infos(
    ids: &mut Vec<usize>,
    node_map: &HashMap<usize, Node>,
    incoming_edges: &mut HashMap<usize, Vec<usize>>,
    outgoing_edges: &mut HashMap<usize, Vec<usize>>,
) -> Result<Vec<Vec<StreamInfo>>> {
    let mut streams = Vec::new();
    let mut i = 0;
    while ids.len() > 0 {
        let source_ids = ids
            .iter()
            .filter(|node_id| !incoming_edges.contains_key(*node_id))
            .copied()
            .collect::<Vec<usize>>();

        let mut sub_streams = Vec::new();

        for source_id in source_ids.iter() {
            let osi = get_stream_info(*source_id, node_map, &incoming_edges, &outgoing_edges)?;
            ids.retain(|id| !osi.ids.contains(id));
            remove_incoming_edge(&osi.last_id, incoming_edges, outgoing_edges);
            sub_streams.push(osi);
        }
        streams.push(sub_streams);
        if i > 5 {
            break;
        }
        i += 1;
    }
    Ok(streams)
}

fn get_stream_info(
    id: usize,
    node_map: &HashMap<usize, Node>,
    incoming_edges: &HashMap<usize, Vec<usize>>,
    outgoing_edges: &HashMap<usize, Vec<usize>>,
) -> Result<StreamInfo> {
    let mut osi = StreamInfo {
        id,
        first_id: id,
        last_id: id,
        ids: Vec::new(),
        r#type: NodeType::Operator,
    };
    osi.ids.push(id);

    if let Some(node) = node_map.get(&id) {
        match node.r#type {
            NodeType::DeviceSource => {
                osi.r#type = NodeType::DeviceSource;
                return Ok(osi);
            }
            NodeType::AppSource => {
                osi.r#type = NodeType::AppSource;
                return Ok(osi);
            }
            NodeType::DeviceSink => {
                osi.r#type = NodeType::DeviceSink;
                return Ok(osi);
            }
            NodeType::AppSink => {
                osi.r#type = NodeType::AppSink;
                return Ok(osi);
            }
            NodeType::Merge => {}
            NodeType::Window => {}
            _ => {}
        }
    }

    let mut current_id = id;
    loop {
        match outgoing_edges.get(&current_id) {
            Some(outgoing_nodes) => {
                if outgoing_nodes.len() == 1 {
                    current_id = outgoing_nodes[0];
                    match (
                        outgoing_edges.get(&current_id),
                        incoming_edges.get(&current_id),
                    ) {
                        (Some(outgoing_nodes), Some(incoming_nodes)) => {
                            if outgoing_nodes.len() == 1 && incoming_nodes.len() == 1 {
                                if let Some(node) = node_map.get(&current_id) {
                                    // match node.r#type.as_str() {
                                    //     "source" | "sink" | "window" | "join" | "merge" => {
                                    //         break;
                                    //     }
                                    //     _ => {}
                                    // }
                                }
                                osi.last_id = current_id;
                                osi.ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                        (Some(outgoing_nodes), None) => {
                            if outgoing_nodes.len() == 1 {
                                if let Some(node) = node_map.get(&current_id) {
                                    // match node.r#type.as_str() {
                                    //     "source" | "sink" | "window" | "join" | "merge" => {
                                    //         break;
                                    //     }
                                    //     _ => {}
                                    // }
                                }
                                osi.last_id = current_id;
                                osi.ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                        (None, Some(incoming_nodes)) => {
                            if incoming_nodes.len() == 1 {
                                if let Some(node) = node_map.get(&current_id) {
                                    // match node.r#type.as_str() {
                                    //     "source" | "sink" | "window" | "join" | "merge" => {
                                    //         break;
                                    //     }
                                    //     _ => {}
                                    // }
                                }
                                osi.last_id = current_id;
                                osi.ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                        (None, None) => break,
                    }
                } else {
                    break;
                }
            }
            None => break,
        }
    }

    Ok(osi)
}

fn remove_incoming_edge(
    last_id: &usize,
    incoming_edges: &mut HashMap<usize, Vec<usize>>,
    outgoing_edges: &HashMap<usize, Vec<usize>>,
) {
    if let Some(target_ids) = outgoing_edges.get(&last_id) {
        incoming_edges.remove(&target_ids[0]);
    }
}

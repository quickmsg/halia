use anyhow::Result;
use message::MessageBatch;
// use operators::merge::merge::Merge;
use operators::window::window::Window;
use std::collections::HashMap;
use tokio::sync::broadcast::{self, Receiver};
use tracing::{debug, error};
use types::rule::{CreateGraph, CreateGraphNode, Status};
use uuid::Uuid;

use crate::{sink::GLOBAL_SINK_MANAGER, source::GLOBAL_SOURCE_MANAGER};

use super::stream::Stream;

pub struct Graph {
    pub create_graph: CreateGraph,
    pub name: String,
    pub status: Status,
    pub streams: Vec<Stream>,
    pub rxs: Vec<Receiver<MessageBatch>>,
}

pub fn new(create_graph: &CreateGraph) -> Graph {
    Graph {
        name: create_graph.name.clone(),
        status: Status::Stopped,
        create_graph: create_graph.clone(),
        streams: Vec::new(),
        rxs: Vec::new(),
    }
}

impl Graph {
    pub async fn run(&self) -> Result<()> {
        let (incoming_edges, outgoing_edges) = self.create_graph.get_edges();
        let mut tmp_incoming_edges = incoming_edges.clone();
        let mut tmp_outgoing_edges = outgoing_edges.clone();

        debug!("tmp incoming edges:{:?}", tmp_incoming_edges);
        debug!("tmp outgoing edges:{:?}", tmp_outgoing_edges);

        let mut node_map = HashMap::<usize, CreateGraphNode>::new();
        for node in self.create_graph.nodes.iter() {
            // TODO 去掉clone
            node_map.insert(node.index, node.clone());
        }

        let mut ids = self
            .create_graph
            .nodes
            .iter()
            .map(|node| node.index)
            .collect();
        let streams = Graph::get_operator_streams(
            &mut ids,
            &node_map,
            &mut tmp_incoming_edges,
            &mut tmp_outgoing_edges,
        )?;

        debug!("streams:{streams:?}");

        let mut receivers = HashMap::new();
        debug!("streams:{:?}", streams);

        for stream in streams {
            for osi in stream {
                match osi.r#type {
                    Some(r#type) => match r#type.as_str() {
                        "source" => {
                            if let Some(node) = node_map.get(&osi.first_id) {
                                let receiver = GLOBAL_SOURCE_MANAGER
                                    .get_receiver(Uuid::new_v4(), "xx".to_string())
                                    .await
                                    .unwrap();
                                receivers.insert(osi.first_id, vec![receiver]);
                            }
                        }
                        "window" => {
                            if let Some(source_ids) = incoming_edges.get(&osi.id) {
                                if let Some(source_id) = source_ids.first() {
                                    if let Some(mut node_receivers) = receivers.remove(source_id) {
                                        let rx = node_receivers.remove(0);
                                        let (tx, nrx) = broadcast::channel::<MessageBatch>(10);
                                        receivers.insert(osi.id, vec![nrx]);

                                        if let Some(node) = node_map.get(&osi.id) {
                                            let mut window =
                                                Window::new(node.conf.clone(), rx, tx)?;
                                            tokio::spawn(async move {
                                                window.run().await;
                                            });
                                        }
                                    }
                                }
                            }
                        }
                        "sink" => {
                            if let Some(source_ids) = incoming_edges.get(&osi.id) {
                                if let Some(source_id) = source_ids.first() {
                                    if let Some(mut node_receivers) = receivers.remove(source_id) {
                                        let rx = node_receivers.remove(0);
                                        if let Some(node) = node_map.get(&osi.id) {
                                            GLOBAL_SINK_MANAGER
                                                .insert_rx(&node.id, rx)
                                                .await
                                                .unwrap();
                                        }
                                    }
                                }
                            }
                        }
                        // "merge" => {
                        //     if let Some(source_ids) = incoming_edges.get(&osi.id) {
                        //         debug!("merge source_ids:{:?}, ois.id:{}", source_ids, &osi.id);
                        //         let mut rxs = Vec::new();
                        //         for source_id in source_ids {
                        //             if let Some(mut node_receivers) = receivers.remove(source_id) {
                        //                 let rx = node_receivers.remove(0);
                        //                 rxs.push(rx);
                        //             }
                        //         }
                        //         debug!("source receivers len:{}", rxs.len());
                        //         let (tx, nrx) = broadcast::channel::<MessageBatch>(10);
                        //         receivers.insert(osi.id, vec![nrx]);
                        //         if let Some(node) = node_map.get(&osi.id) {
                        //             match Merge::new(node.conf.clone(), rxs, tx) {
                        //                 Ok(mut merge) => {
                        //                     tokio::spawn(async move {
                        //                         merge.run().await;
                        //                     });
                        //                 }
                        //                 Err(e) => error!("create merge err:{}", e),
                        //             }
                        //         }
                        //     }
                        // }
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
                        _ => unreachable!(),
                    },
                    None => {
                        if let Some(source_ids) = incoming_edges.get(&osi.first_id) {
                            if let Some(source_id) = source_ids.first() {
                                if let Some(mut node_receivers) = receivers.remove(source_id) {
                                    let rx = node_receivers.remove(0);
                                    let (tx, nrx) = broadcast::channel::<MessageBatch>(10);
                                    receivers.insert(osi.last_id, vec![nrx]);

                                    let mut create_graph_nodes = Vec::new();
                                    for id in osi.ids.iter() {
                                        create_graph_nodes.push(node_map.get(id).unwrap());
                                    }

                                    debug!("create_graph_nodes:{:?}", create_graph_nodes);

                                    let stream = Stream::new(rx, &create_graph_nodes, tx);
                                    match stream {
                                        Ok(mut stream) => {
                                            tokio::spawn(async move {
                                                stream.run().await;
                                            });
                                        }
                                        Err(e) => error!("{}", e),
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        return Ok(());
    }

    pub fn stop(&self) -> Result<()> {
        Ok(())
    }

    fn get_operator_streams(
        ids: &mut Vec<usize>,
        node_map: &HashMap<usize, CreateGraphNode>,
        incoming_edges: &mut HashMap<usize, Vec<usize>>,
        outgoing_edges: &mut HashMap<usize, Vec<usize>>,
    ) -> Result<Vec<Vec<OperatorStreamInfo>>> {
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
                let osi = Graph::get_operator_stream(
                    *source_id,
                    node_map,
                    &incoming_edges,
                    &outgoing_edges,
                )?;
                ids.retain(|id| !osi.ids.contains(id));
                Graph::remove_incoming_edge(&osi.last_id, incoming_edges, outgoing_edges);
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

    fn get_operator_stream(
        id: usize,
        node_map: &HashMap<usize, CreateGraphNode>,
        incoming_edges: &HashMap<usize, Vec<usize>>,
        outgoing_edges: &HashMap<usize, Vec<usize>>,
    ) -> Result<OperatorStreamInfo> {
        let mut osi = OperatorStreamInfo {
            id,
            first_id: id,
            last_id: id,
            ids: Vec::new(),
            r#type: None,
        };
        osi.ids.push(id);

        if let Some(node) = node_map.get(&id) {
            match node.r#type.as_str() {
                "source" => {
                    osi.r#type = Some("source".to_string());
                    return Ok(osi);
                }
                "sink" => {
                    osi.r#type = Some("sink".to_string());
                    return Ok(osi);
                }
                "window" => {
                    osi.r#type = Some("window".to_string());
                    return Ok(osi);
                }
                "join" => {
                    osi.r#type = Some("join".to_string());
                    return Ok(osi);
                }
                "merge" => {
                    osi.r#type = Some("merge".to_string());
                    return Ok(osi);
                }
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
                                        match node.r#type.as_str() {
                                            "source" | "sink" | "window" | "join" | "merge" => {
                                                break;
                                            }
                                            _ => {}
                                        }
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
                                        match node.r#type.as_str() {
                                            "source" | "sink" | "window" | "join" | "merge" => {
                                                break;
                                            }
                                            _ => {}
                                        }
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
                                        match node.r#type.as_str() {
                                            "source" | "sink" | "window" | "join" | "merge" => {
                                                break;
                                            }
                                            _ => {}
                                        }
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
}

#[derive(Debug)]
struct OperatorStreamInfo {
    r#type: Option<String>,
    id: usize,
    first_id: usize,
    last_id: usize,
    ids: Vec<usize>,
}

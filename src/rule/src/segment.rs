use std::{collections::HashMap, net::Incoming};

use anyhow::Result;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};
use tracing::debug;
use types::rules::Node;

#[derive(Debug)]
pub struct Segement {
    pub ids: Vec<usize>,
    pub rx: Option<broadcast::Receiver<MessageBatch>>,

    pub single_tx: Option<mpsc::Sender<MessageBatch>>,
    pub broadcast_tx: Option<broadcast::Sender<MessageBatch>>,
}

pub fn take_source_ids(
    ids: &mut Vec<usize>,
    incoming_edges: &mut HashMap<usize, Vec<usize>>,
    outgoing_edges: &mut HashMap<usize, Vec<usize>>,
) -> Vec<usize> {
    let source_ids = ids
        .iter()
        .filter(|node_id| !incoming_edges.contains_key(*node_id))
        .copied()
        .collect::<Vec<usize>>();

    ids.retain(|id| !source_ids.contains(id));
    for source_id in &source_ids {
        remove_incoming_edge(source_id, incoming_edges, outgoing_edges);
    }

    source_ids
}

pub fn get_segments(
    ids: &mut Vec<usize>,
    node_map: &HashMap<usize, Node>,
    incoming_edges: &mut HashMap<usize, Vec<usize>>,
    outgoing_edges: &mut HashMap<usize, Vec<usize>>,
) -> Result<Vec<Vec<Segement>>> {
    let mut rule_segments = Vec::new();
    while ids.len() > 0 {
        let source_ids = ids
            .iter()
            .filter(|node_id| !incoming_edges.contains_key(*node_id))
            .copied()
            .collect::<Vec<usize>>();

        let mut segments = Vec::new();

        for source_id in source_ids.iter() {
            let segment = get_segment(*source_id, node_map, &incoming_edges, &outgoing_edges)?;
            debug!("{:?}", segment);
            ids.retain(|id| !segment.ids.contains(id));
            remove_incoming_edge(&segment.ids.last().unwrap(), incoming_edges, outgoing_edges);
            segments.push(segment);
        }
        rule_segments.push(segments);
    }
    Ok(rule_segments)
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

pub fn get_segment(
    id: usize,
    node_map: &HashMap<usize, Node>,
    incoming_edges: &HashMap<usize, Vec<usize>>,
    outgoing_edges: &HashMap<usize, Vec<usize>>,
) -> Result<Segement> {
    let mut seg = Segement {
        ids: vec![id],
        rx: None,
        single_tx: None,
        broadcast_tx: None,
    };

    let node = node_map.get(&id).unwrap();
    match node.node_type {
        types::rules::NodeType::Merge | types::rules::NodeType::Window => return Ok(seg),
        _ => {}
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
                                let node = node_map.get(&current_id).unwrap();
                                match node.node_type {
                                    types::rules::NodeType::Merge
                                    | types::rules::NodeType::Window => break,
                                    _ => {}
                                }
                                seg.ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                        (None, Some(incoming_nodes)) => {
                            if incoming_nodes.len() == 1 {
                                let node = node_map.get(&current_id).unwrap();
                                match node.node_type {
                                    types::rules::NodeType::Merge
                                    | types::rules::NodeType::Window => break,
                                    _ => {}
                                }
                                seg.ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                        (Some(outgoing_nodes), None) => {
                            if outgoing_nodes.len() == 1 {
                                let node = node_map.get(&current_id).unwrap();
                                match node.node_type {
                                    types::rules::NodeType::Merge
                                    | types::rules::NodeType::Window => break,
                                    _ => {}
                                }
                                seg.ids.push(current_id);
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

    Ok(seg)
}

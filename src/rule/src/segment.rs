use std::collections::HashMap;

use anyhow::Result;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};
use types::rules::Node;

#[derive(Debug)]
pub struct Segement {
    first_id: usize,
    ids: Vec<usize>,
    last_id: usize,
    single_rx: Option<broadcast::Receiver<MessageBatch>>,
    broadcast_rx: Option<broadcast::Receiver<MessageBatch>>,

    single_tx: Option<mpsc::Sender<MessageBatch>>,
    broadcast_tx: Option<broadcast::Sender<MessageBatch>>,
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
            ids.retain(|id| !segment.ids.contains(id));
            remove_incoming_edge(&segment.last_id, incoming_edges, outgoing_edges);
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
        first_id: id,
        ids: vec![],
        last_id: id,
        single_rx: None,
        broadcast_rx: None,
        single_tx: None,
        broadcast_tx: None,
    };

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
                                    match node.node_type {
                                        types::rules::NodeType::Merge
                                        | types::rules::NodeType::Window => break,
                                        _ => {}
                                    }
                                }
                                seg.last_id = current_id;
                                seg.ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                        (Some(outgoing_nodes), None) => {
                            if outgoing_nodes.len() == 1 {
                                if let Some(node) = node_map.get(&current_id) {
                                    match node.node_type {
                                        types::rules::NodeType::Merge => break,
                                        _ => {}
                                    }
                                }
                                seg.last_id = current_id;
                                seg.ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                        (None, Some(incoming_nodes)) => {
                            if incoming_nodes.len() == 1 {
                                seg.last_id = current_id;
                                seg.ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                        (None, None) => unreachable!(),
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

// pub(crate) async fn start_stream(
//     nodes: Vec<&CreateRuleNode>,
//     mut rx: broadcast::Receiver<MessageBatch>,
//     tx: broadcast::Sender<MessageBatch>,
//     mut stop_signal: broadcast::Receiver<()>,
// ) {
//     let mut functions = Vec::new();
//     for create_graph_node in nodes {
//         let node = functions::new(&create_graph_node).unwrap();
//         functions.push(node);
//     }

//     tokio::spawn(async move {
//         loop {
//             select! {
//                 biased;

//                 _ = stop_signal.recv() => {
//                     debug!("stream stop");
//                     return
//                 }

//                 message_batch = rx.recv() => {
//                     match message_batch {
//                         Ok(mut message_batch) => {
//                             for function in &functions {
//                                 function.call(&mut message_batch);
//                                 if message_batch.len() == 0 {
//                                     break;
//                                 }
//                             }

//                             if message_batch.len() != 0 {
//                                 if let Err(e) = tx.send(message_batch) {
//                                     error!("stream send err:{}, ids", e);
//                                 }
//                             }
//                         },
//                         Err(e) => {
//                             error!("stream recv err:{}", e);
//                             break;
//                         }
//                     }
//                 }
//             }
//         }
//     });
// }

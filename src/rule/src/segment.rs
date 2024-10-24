use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use functions::Function;
use message::RuleMessageBatch;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tracing::debug;
use types::rules::Node;

pub fn start_segment(
    mut rx: mpsc::UnboundedReceiver<RuleMessageBatch>,
    functions: Vec<Box<dyn Function>>,
    txs: Vec<mpsc::UnboundedSender<RuleMessageBatch>>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) {
    tokio::spawn(async move {
        loop {
            select! {
                Some(mb) = rx.recv() => {
                    handle_segment_mb(mb, &functions, &txs).await;
                }
                _ = stop_signal_rx.recv() => {
                    return
                }
            }
        }
    });
}

async fn handle_segment_mb(
    mb: RuleMessageBatch,
    functions: &Vec<Box<dyn Function>>,
    txs: &Vec<mpsc::UnboundedSender<RuleMessageBatch>>,
) {
    let mut mb = mb.take_mb();
    for function in functions {
        if !function.call(&mut mb).await {
            return;
        }
    }

    match txs.len() {
        0 => {}
        1 => {
            let _ = txs[0].send(RuleMessageBatch::Owned(mb));
        }
        _ => {
            let mb = Arc::new(mb);
            for tx in txs.iter() {
                let _ = tx.send(RuleMessageBatch::Arc(mb.clone()));
            }
        }
    }
}

/// 将最开始的节点取出，并从ids里面删除
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

pub fn get_3d_ids(
    ids: &mut Vec<usize>,
    node_map: &HashMap<usize, Node>,
    incoming_edges: &mut HashMap<usize, Vec<usize>>,
    outgoing_edges: &mut HashMap<usize, Vec<usize>>,
) -> Result<Vec<Vec<Vec<usize>>>> {
    let mut threed_ids = Vec::new();
    let mut i = 0;
    while ids.len() > 0 {
        i += 1;
        if i == 3 {
            return Ok(threed_ids);
        }
        let source_ids = ids
            .iter()
            .filter(|node_id| !incoming_edges.contains_key(*node_id))
            .copied()
            .collect::<Vec<usize>>();

        debug!("{:?}", source_ids);

        let mut twod_ids = vec![];
        for source_id in source_ids.iter() {
            debug!(
                "{:?}, {:?} {:?} {:?}",
                source_id, node_map, incoming_edges, outgoing_edges
            );
            let oned_ids = get_ids(*source_id, node_map, &incoming_edges, &outgoing_edges)?;
            debug!("{:?}", oned_ids);
            ids.retain(|id| !oned_ids.contains(id));
            remove_incoming_edge(&oned_ids.last().unwrap(), incoming_edges, outgoing_edges);
            twod_ids.push(oned_ids);
        }
        threed_ids.push(twod_ids);
    }
    Ok(threed_ids)
}

fn remove_incoming_edge(
    last_id: &usize,
    incoming_edges: &mut HashMap<usize, Vec<usize>>,
    outgoing_edges: &HashMap<usize, Vec<usize>>,
) {
    if let Some(target_ids) = outgoing_edges.get(&last_id) {
        for target_id in target_ids {
            incoming_edges.remove(target_id);
        }
    }
}

pub fn get_ids(
    id: usize,
    node_map: &HashMap<usize, Node>,
    incoming_edges: &HashMap<usize, Vec<usize>>,
    outgoing_edges: &HashMap<usize, Vec<usize>>,
) -> Result<Vec<usize>> {
    debug!("{}", id);
    let mut ids = vec![id];

    let node = node_map.get(&id).unwrap();
    match node.node_type {
        types::rules::NodeType::Merge | types::rules::NodeType::Window => return Ok(ids),
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
                                ids.push(current_id);
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
                                ids.push(current_id);
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
                                ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                        // TODO fix this
                        (None, None) => break,
                    }
                } else {
                    break;
                }
            }
            None => break,
        }
    }

    Ok(ids)
}

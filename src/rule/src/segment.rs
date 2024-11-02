use std::{collections::HashMap, sync::Arc};

use functions::Function;
use futures::StreamExt;
use message::RuleMessageBatch;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::debug;
use types::rules::Node;

pub(crate) fn start_segment(
    rxs: Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>,
    functions: Vec<Box<dyn Function>>,
    txs: Vec<mpsc::UnboundedSender<RuleMessageBatch>>,
    mut stop_signal_rx: broadcast::Receiver<()>,
) {
    let streams: Vec<_> = rxs
        .into_iter()
        .map(|rx| UnboundedReceiverStream::new(rx))
        .collect();

    let mut stream = futures::stream::select_all(streams);

    tokio::spawn(async move {
        loop {
            select! {
                Some(mb) = stream.next() => {
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
pub(crate) fn take_source_ids(
    ids: &mut Vec<usize>,
    incoming_edges: &mut HashMap<usize, Vec<usize>>,
    outgoing_edges: &mut HashMap<usize, Vec<usize>>,
) -> Vec<usize> {
    let source_ids: Vec<_> = ids
        .iter()
        .filter(|node_id| !incoming_edges.contains_key(*node_id))
        .copied()
        .collect();

    ids.retain(|id| !source_ids.contains(id));
    for source_id in &source_ids {
        remove_incoming_edge(source_id, incoming_edges, outgoing_edges);
    }

    source_ids
}

// 不要去除log的id
pub(crate) fn take_sink_ids(
    ids: &mut Vec<usize>,
    incoming_edges: &mut HashMap<usize, Vec<usize>>,
    outgoing_edges: &mut HashMap<usize, Vec<usize>>,
) -> Vec<usize> {
    let sink_ids: Vec<_> = ids
        .iter()
        .filter(|node_id| !outgoing_edges.contains_key(*node_id))
        .copied()
        .collect();

    ids.retain(|id| !sink_ids.contains(id));
    for sink_id in &sink_ids {
        remove_incoming_edge(sink_id, incoming_edges, outgoing_edges);
    }

    sink_ids
}

pub(crate) fn get_segments(
    ids: &mut Vec<usize>,
    node_map: &HashMap<usize, Node>,
    incoming_edges: &mut HashMap<usize, Vec<usize>>,
    outgoing_edges: &mut HashMap<usize, Vec<usize>>,
) -> Vec<Vec<usize>> {
    debug!("{:?}", incoming_edges);
    debug!("{:?}", outgoing_edges);
    let mut segments = vec![];
    let mut i = 0;
    while ids.len() > 0 {
        i += 1;
        if i == 3 {
            return segments;
        }
        let source_ids: Vec<_> = ids
            .iter()
            .filter(|node_id| !incoming_edges.contains_key(*node_id))
            .copied()
            .collect();
        debug!("source_ids: {:?}", source_ids);
        for source_id in source_ids.iter() {
            let segment_ids =
                get_segment_ids(*source_id, node_map, &incoming_edges, &outgoing_edges);
            ids.retain(|id| !segment_ids.contains(id));
            remove_incoming_edge(&segment_ids.last().unwrap(), incoming_edges, outgoing_edges);
            segments.push(segment_ids);
        }
        debug!("here");
    }

    segments
}

fn get_segment_ids(
    source_id: usize,
    node_map: &HashMap<usize, Node>,
    incoming_edges: &HashMap<usize, Vec<usize>>,
    outgoing_edges: &HashMap<usize, Vec<usize>>,
) -> Vec<usize> {
    let mut ids = vec![source_id];

    let node = node_map.get(&source_id).unwrap();
    match node.node_type {
        types::rules::NodeType::Merge | types::rules::NodeType::Window => ids,
        _ => {
            let mut current_id = source_id;
            loop {
                if let Some(outgoing_nodes) = outgoing_edges.get(&current_id) {
                    if outgoing_nodes.len() == 1 {
                        current_id = outgoing_nodes[0];
                        if let Some(incoming_nodes) = incoming_edges.get(&current_id) {
                            if incoming_nodes.len() == 1 {
                                let node = node_map.get(&current_id).unwrap();
                                match node.node_type {
                                    types::rules::NodeType::Merge
                                    | types::rules::NodeType::Window
                                    | types::rules::NodeType::Databoard => break,
                                    _ => {}
                                }
                                ids.push(current_id);
                            } else {
                                break;
                            }
                        }
                    }
                } else {
                    break;
                }
            }
            ids
        }
    }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use types::rules::{Node, NodeType};

    use super::{get_segment_ids, get_segments};

    fn generate_test_data() -> (
        Vec<usize>,
        HashMap<usize, Node>,
        HashMap<usize, Vec<usize>>,
        HashMap<usize, Vec<usize>>,
    ) {
        let ids = vec![0, 1, 2, 3];
        let mut node_map = HashMap::new();
        node_map.insert(
            0,
            Node {
                index: 0,
                node_type: NodeType::Computer,
                conf: serde_json::Value::Null,
            },
        );
        node_map.insert(
            1,
            Node {
                index: 1,
                node_type: NodeType::Computer,
                conf: serde_json::Value::Null,
            },
        );
        node_map.insert(
            2,
            Node {
                index: 2,
                node_type: NodeType::Merge,
                conf: serde_json::Value::Null,
            },
        );
        node_map.insert(
            3,
            Node {
                index: 3,
                node_type: NodeType::Computer,
                conf: serde_json::Value::Null,
            },
        );
        let mut incoming_edges = HashMap::new();
        incoming_edges.insert(1, vec![0]);
        incoming_edges.insert(2, vec![1, 3]);
        let mut outgoing_edges = HashMap::new();
        outgoing_edges.insert(0, vec![1]);
        outgoing_edges.insert(1, vec![2]);
        outgoing_edges.insert(3, vec![2]);

        (ids, node_map, incoming_edges, outgoing_edges)
    }

    #[test]
    fn test_get_segment_ids() {
        let (_, node_map, incoming_edges, outgoing_edges) = generate_test_data();
        let segment_ids = get_segment_ids(0, &node_map, &incoming_edges, &outgoing_edges);
        assert_eq!(segment_ids, vec![0, 1]);
        let segment_ids = get_segment_ids(3, &node_map, &incoming_edges, &outgoing_edges);
        assert_eq!(segment_ids, vec![3]);
    }

    #[test]
    fn test_get_segments() {
        let (mut ids, node_map, mut incoming_edges, mut outgoing_edges) = generate_test_data();
        let segments = get_segments(
            &mut ids,
            &node_map,
            &mut incoming_edges,
            &mut outgoing_edges,
        );
        assert_eq!(segments, vec![vec![0, 1], vec![3], vec![2]]);
    }
}

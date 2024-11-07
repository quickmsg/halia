use std::collections::HashMap;

use message::RuleMessageBatch;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::debug;
use types::rules::{Conf, NodeType};

pub struct Graph {
    ids: Vec<usize>,
    mut_ids: Vec<usize>,
    mut_incoming_edges: HashMap<usize, Vec<usize>>,
    incoming_edges: HashMap<usize, Vec<usize>>,
    mut_outgoing_edges: HashMap<usize, Vec<usize>>,
    outgoing_edges: HashMap<usize, Vec<usize>>,
    node_types: HashMap<usize, NodeType>,
}

impl Graph {
    pub fn new(conf: &Conf) -> Self {
        let (incoming_edges, outgoing_edges) = Self::get_edges(&conf);
        let mut_incoming_edges = incoming_edges.clone();
        let mut_outgoing_edges = outgoing_edges.clone();
        let ids = Self::get_ids(&conf);
        let mut_ids = ids.clone();

        let node_types = conf
            .nodes
            .iter()
            .map(|node| (node.index, node.node_type.clone()))
            .collect();

        Self {
            ids,
            mut_ids,
            incoming_edges,
            mut_incoming_edges,
            outgoing_edges,
            mut_outgoing_edges,
            node_types,
        }
    }

    pub fn validate(&self) -> bool {
        todo!()
    }

    fn get_edges(conf: &Conf) -> (HashMap<usize, Vec<usize>>, HashMap<usize, Vec<usize>>) {
        let mut incoming_edges = HashMap::new();
        let mut outgoing_edges = HashMap::new();

        for edge in conf.edges.iter() {
            incoming_edges
                .entry(edge.target)
                .or_insert_with(Vec::new)
                .push(edge.source);
            outgoing_edges
                .entry(edge.source)
                .or_insert_with(Vec::new)
                .push(edge.target);
        }

        (incoming_edges, outgoing_edges)
    }

    fn get_ids(conf: &Conf) -> Vec<usize> {
        conf.nodes.iter().map(|node| node.index).collect()
    }

    pub fn get_outgoing_edges(&self) -> &HashMap<usize, Vec<usize>> {
        &self.outgoing_edges
    }

    pub fn get_incoming_edges(&self) -> &HashMap<usize, Vec<usize>> {
        &self.incoming_edges
    }

    /// 将最开始的节点取出，并从ids里面删除
    pub fn take_source_indexes(&mut self) -> Vec<usize> {
        let indexes: Vec<_> = self
            .mut_ids
            .iter()
            .filter(|index| !self.mut_incoming_edges.contains_key(*index))
            .copied()
            .collect();

        self.mut_ids.retain(|index| !indexes.contains(index));
        for index in &indexes {
            self.remove_incoming_edge(index);
        }

        indexes
    }

    pub fn get_output_cnt_by_index(&self, index: usize) -> usize {
        self.outgoing_edges.get(&index).unwrap().len()
    }

    pub fn get_input_cnt_by_index(&self, index: usize) -> usize {
        self.incoming_edges.get(&index).unwrap().len()
    }

    pub fn take_sink_indexes(&mut self) -> Vec<usize> {
        let sink_ids: Vec<_> = self
            .mut_ids
            .iter()
            .filter(|node_id| !self.outgoing_edges.contains_key(*node_id))
            .copied()
            .collect();

        self.mut_ids.retain(|id| !sink_ids.contains(id));
        for sink_id in &sink_ids {
            self.remove_incoming_edge(sink_id);
        }

        sink_ids
    }

    pub fn remove_incoming_edge(&mut self, last_id: &usize) {
        if let Some(target_ids) = self.mut_outgoing_edges.get(&last_id) {
            for target_id in target_ids {
                self.mut_incoming_edges.remove(target_id);
            }
        }
    }

    pub fn get_rxs(
        &self,
        index: &usize,
        receivers: &mut HashMap<usize, Vec<UnboundedReceiver<RuleMessageBatch>>>,
        senders: &mut HashMap<usize, Vec<UnboundedSender<RuleMessageBatch>>>,
    ) -> Vec<UnboundedReceiver<RuleMessageBatch>> {
        let mut rxs = vec![];

        let source_indexes = self.incoming_edges.get(index).unwrap();
        for index in source_indexes {
            if let Some(exist_rxs) = receivers.get_mut(index) {
                if let Some(rx) = exist_rxs.pop() {
                    rxs.push(rx);
                    continue;
                }
            }
            let (tx, rx) = unbounded_channel();
            senders.entry(*index).or_insert_with(Vec::new).push(tx);
            rxs.push(rx);
        }

        rxs
    }

    pub fn get_txs(
        &self,
        index: &usize,
        receivers: &mut HashMap<usize, Vec<UnboundedReceiver<RuleMessageBatch>>>,
        senders: &mut HashMap<usize, Vec<UnboundedSender<RuleMessageBatch>>>,
    ) -> Vec<UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];

        let sink_indexes = self.outgoing_edges.get(index).unwrap();
        for index in sink_indexes {
            if let Some(exist_txs) = senders.get_mut(index) {
                if let Some(tx) = exist_txs.pop() {
                    txs.push(tx);
                    continue;
                }
            }

            let (tx, rx) = unbounded_channel();
            receivers.entry(*index).or_insert_with(Vec::new).push(rx);
            txs.push(tx);
        }

        txs
    }

    pub fn get_segments(&mut self) -> Vec<Vec<usize>> {
        let mut segments = vec![];
        while self.mut_ids.len() > 0 {
            let source_ids: Vec<_> = self
                .mut_ids
                .iter()
                .filter(|node_id| !self.mut_incoming_edges.contains_key(*node_id))
                .copied()
                .collect();
            debug!("source_ids: {:?}", source_ids);
            for source_id in source_ids.iter() {
                let segment_ids = self.get_segment_ids(*source_id);
                self.mut_ids.retain(|id| !segment_ids.contains(id));
                self.remove_incoming_edge(&segment_ids.last().unwrap());
                segments.push(segment_ids);
            }
        }

        segments
    }

    fn get_segment_ids(&self, source_id: usize) -> Vec<usize> {
        let mut ids = vec![source_id];

        match self.node_types.get(&source_id).unwrap() {
            types::rules::NodeType::Merge | types::rules::NodeType::Window => ids,
            _ => {
                let mut current_id = source_id;
                loop {
                    if let Some(outgoing_nodes) = self.mut_outgoing_edges.get(&current_id) {
                        if outgoing_nodes.len() == 1 {
                            current_id = outgoing_nodes[0];
                            if let Some(incoming_nodes) = self.mut_incoming_edges.get(&current_id) {
                                if incoming_nodes.len() == 1 {
                                    match self.node_types.get(&current_id).unwrap() {
                                        types::rules::NodeType::Merge
                                        | types::rules::NodeType::Window
                                        | types::rules::NodeType::Databoard
                                        | types::rules::NodeType::BlackHole
                                        | types::rules::NodeType::DeviceSink
                                        | types::rules::NodeType::AppSink => break,
                                        _ => {}
                                    }
                                    ids.push(current_id);
                                    continue;
                                }
                            }
                        }
                    }

                    break;
                }
                ids
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph() {
        let conf = Conf {
            nodes: vec![
                types::rules::Node {
                    index: 0,
                    node_type: types::rules::NodeType::DeviceSource,
                    conf: serde_json::json!({}),
                },
                types::rules::Node {
                    index: 1,
                    node_type: types::rules::NodeType::AppSource,
                    conf: serde_json::json!({}),
                },
                types::rules::Node {
                    index: 2,
                    node_type: types::rules::NodeType::DeviceSink,
                    conf: serde_json::json!({}),
                },
                types::rules::Node {
                    index: 3,
                    node_type: types::rules::NodeType::AppSink,
                    conf: serde_json::json!({}),
                },
                types::rules::Node {
                    index: 4,
                    node_type: types::rules::NodeType::Databoard,
                    conf: serde_json::json!({}),
                },
                types::rules::Node {
                    index: 5,
                    node_type: types::rules::NodeType::BlackHole,
                    conf: serde_json::json!({}),
                },
                types::rules::Node {
                    index: 6,
                    node_type: types::rules::NodeType::Merge,
                    conf: serde_json::json!({}),
                },
                types::rules::Node {
                    index: 7,
                    node_type: types::rules::NodeType::Window,
                    conf: serde_json::json!({}),
                },
                types::rules::Node {
                    index: 8,
                    node_type: types::rules::NodeType::Filter,
                    conf: serde_json::json!({}),
                },
                types::rules::Node {
                    index: 9,
                    node_type: types::rules::NodeType::Computer,
                    conf: serde_json::json!({}),
                },
            ],
            edges: vec![
                types::rules::Edge {
                    source: 0,
                    target: 6,
                },
                types::rules::Edge {
                    source: 1,
                    target: 6,
                },
                types::rules::Edge {
                    source: 6,
                    target: 7,
                },
                types::rules::Edge {
                    source: 7,
                    target: 8,
                },
                types::rules::Edge {
                    source: 8,
                    target: 9,
                },
                types::rules::Edge {
                    source: 9,
                    target: 2,
                },
                types::rules::Edge {
                    source: 9,
                    target: 3,
                },
                types::rules::Edge {
                    source: 9,
                    target: 4,
                },
                types::rules::Edge {
                    source: 9,
                    target: 5,
                },
            ],
        };

        let mut graph = Graph::new(&conf);

        let mut source_indexes = graph.take_source_indexes();
        source_indexes.sort();
        assert_eq!(source_indexes, vec![0, 1]);

        let mut sink_indexes = graph.take_sink_indexes();
        sink_indexes.sort();
        assert_eq!(sink_indexes, vec![2, 3, 4, 5]);

        let mut segments = graph.get_segments();
        segments.iter_mut().for_each(|segment| segment.sort());

        let segment = segments.iter().find(|segment| segment.contains(&6));
        assert_eq!(segment, Some(&vec![6]));

        let segment = segments.iter().find(|segment| segment.contains(&7));
        assert_eq!(segment, Some(&vec![7]));

        let segment = segments.iter().find(|segment| segment.contains(&8));
        assert_eq!(segment, Some(&vec![8, 9]));
    }
}

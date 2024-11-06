use std::collections::HashMap;

use message::RuleMessageBatch;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use types::rules::Conf;

pub struct Graph {
    source_ids: Vec<usize>,
    sink_ids: Vec<usize>,
    conf: Conf,
}

impl Graph {
    pub fn new(conf: Conf) -> Self {
        Self {
            source_ids: vec![],
            sink_ids: vec![],
            conf,
        }
    }

    fn get_edges(&self) {
        let mut incoming_edges = HashMap::new();
        let mut outgoing_edges = HashMap::new();

        for edge in self.conf.edges.iter() {
            incoming_edges
                .entry(edge.target)
                .or_insert_with(Vec::new)
                .push(edge.source);
            outgoing_edges
                .entry(edge.source)
                .or_insert_with(Vec::new)
                .push(edge.target);
        }

        // (incoming_edges, outgoing_edges)
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
            // remove_incoming_edge(source_id, incoming_edges, outgoing_edges);
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
            // remove_incoming_edge(sink_id, incoming_edges, outgoing_edges);
        }

        sink_ids
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

    fn get_rxs(
        index: &usize,
        incoming_edges: &HashMap<usize, Vec<usize>>,
        receivers: &mut HashMap<usize, Vec<UnboundedReceiver<RuleMessageBatch>>>,
        senders: &mut HashMap<usize, Vec<UnboundedSender<RuleMessageBatch>>>,
    ) -> Vec<UnboundedReceiver<RuleMessageBatch>> {
        let mut rxs = vec![];

        let source_ids = incoming_edges.get(index).unwrap();
        for source_id in source_ids {
            if let Some(exist_rxs) = receivers.get_mut(source_id) {
                if let Some(rx) = exist_rxs.pop() {
                    rxs.push(rx);
                }
            } else {
                let (tx, rx) = unbounded_channel();
                rxs.push(rx);
                todo!()
            }
        }

        rxs
    }

    fn get_txs(
        outgoing_edges: &HashMap<usize, Vec<usize>>,
        receivers: &mut HashMap<usize, Vec<UnboundedReceiver<RuleMessageBatch>>>,
        senders: &mut HashMap<usize, Vec<UnboundedSender<RuleMessageBatch>>>,
    ) -> Vec<UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];
        for (_, tx) in outgoing_edges {
            for _ in tx {
                let (tx, rx) = unbounded_channel();
                txs.push(tx);
            }
        }
        txs
    }
}

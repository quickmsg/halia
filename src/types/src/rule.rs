use message::MessageBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateGraph {
    pub name: String,
    pub nodes: Vec<CreateGraphNode>,
    pub edges: Vec<CreateGraphEdge>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateGraphNode {
    pub id: usize,
    pub r#type: String,
    pub name: String,
    pub conf: Value,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateGraphEdge {
    pub source: usize,
    pub target: usize,
}

impl CreateGraph {
    pub fn get_edges(&self) -> (HashMap<usize, Vec<usize>>, HashMap<usize, Vec<usize>>) {
        let mut incoming_edges = HashMap::new();
        let mut outgoing_edges = HashMap::new();

        for edge in self.edges.iter() {
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

    pub fn get_ids_by_type(&self, r#type: &str) -> Vec<usize> {
        self.nodes
            .iter()
            .filter(|node| node.r#type.as_str() == r#type)
            .map(|node| node.id)
            .collect()
    }

    pub fn get_operator_ids(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .filter(|node| match node.r#type.as_str() {
                "source" => false,
                "sink" => false,
                // "window" => false,
                _ => true,
            })
            .map(|node| node.id)
            .collect()
    }
}

pub enum Node {
    // Operate(Box<dyn Operate>),
    Filter,
}

pub trait Operate: Send + Sync {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateSink {
    pub r#type: String,
    pub name: String,
    pub encode: String,
    pub conf: serde_json::Value,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateSource {
    pub r#type: String,
    pub name: String,
    pub format: String,
    pub conf: serde_json::Value,
}

#[derive(Debug, PartialEq)]
pub enum Status {
    Running,
    Stopped,
}

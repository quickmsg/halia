use message::MessageBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateRuleReq {
    pub name: String,
    pub nodes: Vec<CreateRuleNode>,
    pub edges: Vec<CreateRuleEdge>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateRuleNode {
    pub index: usize,
    pub r#type: RuleNodeType,
    pub conf: Value,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum RuleNodeType {
    Source,
    Merge,
    Window,
    Operator,
    Sink,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateRuleSource {
    pub r#type: CreateRuleSourceType,
    pub id: Uuid,
    pub source_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum CreateRuleSourceType {
    Device(String),
    App(String),
}

#[derive(Serialize, Deserialize)]
pub struct CreateRuleSink {
    pub r#type: CreateRuleSinkType,
    pub id: Uuid,
    pub sink_id: Option<Uuid>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreateRuleSinkType {
    Device(String),
    App,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateRuleEdge {
    pub source: usize,
    pub target: usize,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListRuleResp {
    pub id: Uuid,
    pub name: String,
}

impl CreateRuleReq {
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

    // pub fn get_ids_by_type(&self, r#type: &str) -> Vec<Uuid> {
    //     self.nodes
    //         .iter()
    //         .filter(|node| node.r#type.as_str() == r#type)
    //         .map(|node| node.id.unwrap())
    //         .collect()
    // }

    // pub fn get_operator_ids(&self) -> Vec<Uuid> {
    //     self.nodes
    //         .iter()
    //         .filter(|node| match node.r#type.as_str() {
    //             "source" => false,
    //             "sink" => false,
    //             // "window" => false,
    //             _ => true,
    //         })
    //         .map(|node| node.id.unwrap())
    //         .collect()
    // }
}

pub enum Node {
    // Operate(Box<dyn Operate>),
    Filter,
}

pub trait Operate: Send + Sync {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool;
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

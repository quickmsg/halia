use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

pub mod apps;
pub mod devices;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateUpdateRuleReq {
    pub name: String,
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub desc: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Node {
    pub index: usize,
    pub r#type: NodeType,
    pub conf: Value,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Edge {
    pub source: usize,
    pub target: usize,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    DeviceSource,
    AppSource,
    Merge,
    Window,
    Operator,
    DeviceSink,
    AppSink,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SourceNode {
    pub r#type: String,
    #[serde(flatten)]
    pub conf: Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SinkNode {
    pub r#type: String,
    pub conf: Value,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WindowConf {
    #[serde(rename = "type")]
    pub typ: String,
    pub count: Option<u64>,
    // s
    pub interval: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct CreateUpdateRuleSink {
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

#[derive(Deserialize, Serialize, Debug)]
pub struct ListRuleResp {
    pub id: Uuid,
    pub name: String,
}

impl CreateUpdateRuleReq {
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

#[derive(Serialize)]
pub struct SearchRulesResp {
    pub total: usize,
    pub data: Vec<SearchRulesItemResp>,
}

#[derive(Serialize)]
pub struct SearchRulesItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateRuleReq,
}

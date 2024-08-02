use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

use crate::BaseConf;

pub mod apps;
pub mod devices;
pub mod functions;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateUpdateRuleReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: RuleConf,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct RuleConf {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Node {
    pub index: usize,
    pub node_type: NodeType,
    #[serde(flatten)]
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
    Filter,
    Operator,
    Computer,
    DeviceSink,
    AppSink,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SourceNode {
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(flatten)]
    pub conf: Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SinkNode {
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(flatten)]
    pub conf: Value,
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

        for edge in self.ext.edges.iter() {
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
    pub on: bool,
}

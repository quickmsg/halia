use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

use crate::BaseConf;

pub mod apps;
pub mod devices;
pub mod functions;
pub mod databoard;

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
    pub running_cnt: usize,
    pub off_cnt: usize,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub on: Option<bool>,
}

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
    Databoard,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeviceSourceNode {
    pub device_id: Uuid,
    pub source_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppSourceNode {
    pub app_id: Uuid,
    pub source_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeviceSinkNode {
    pub device_id: Uuid,
    pub sink_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppSinkNode {
    pub app_id: Uuid,
    pub sink_id: Uuid,
}

#[derive(Deserialize, Serialize)]
pub struct DataboardNode {
    pub databoard_id: Uuid,
    pub data_id: Uuid,
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
    pub on: bool,
    pub conf: CreateUpdateRuleReq,
}

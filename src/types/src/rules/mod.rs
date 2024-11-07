use message::MessageBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

pub mod apps;
pub mod databoard;
pub mod devices;
pub mod functions;

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
    pub on: usize,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub on: Option<bool>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateUpdateRuleReq {
    pub name: String,
    pub conf: Conf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Conf {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Node {
    pub index: usize,
    pub node_type: NodeType,
    #[serde(flatten)]
    pub conf: Value,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub struct Edge {
    pub source: usize,
    pub target: usize,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum NodeType {
    DeviceSource,
    AppSource,
    Merge,
    Window,
    Filter,
    Computer,
    DeviceSink,
    AppSink,
    Databoard,
    BlackHole,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeviceSourceNode {
    pub device_id: String,
    pub source_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppSourceNode {
    pub app_id: String,
    pub source_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DeviceSinkNode {
    pub device_id: String,
    pub sink_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppSinkNode {
    pub app_id: String,
    pub sink_id: String,
}

#[derive(Deserialize, Serialize)]
pub struct DataboardNode {
    pub databoard_id: String,
    pub data_id: String,
}

#[derive(Deserialize, Serialize)]
pub struct LogNode {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
pub struct CreateUpdateRuleSink {
    pub r#type: CreateRuleSinkType,
    pub id: String,
    pub sink_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CreateRuleSinkType {
    Device(String),
    App,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListRuleResp {
    pub id: String,
    pub name: String,
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
    pub id: String,
    pub on: bool,
    pub log_enable: bool,
    pub conf: CreateUpdateRuleReq,
}

#[derive(Serialize)]
pub struct ReadRuleNodeResp {
    pub index: usize,
    pub data: serde_json::Value,
}

// 规则引擎中传递的消息，避免内存消耗
#[derive(Clone)]
pub enum MessageBatchType {
    Arc(Arc<MessageBatch>),
    Owned(MessageBatch),
}

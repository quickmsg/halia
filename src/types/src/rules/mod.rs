use message::MessageBatch;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use crate::Status;

pub mod apps;
pub mod databoard;
pub mod devices;
pub mod functions;

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
    pub on: usize,
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub status: Option<Status>,
    pub parent_id: Option<String>,
    pub resource_id: Option<String>,
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
    Aggregation,
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

#[derive(Serialize)]
pub struct ListRulesResp {
    pub count: usize,
    pub list: Vec<ListRulesItem>,
}

#[derive(Serialize)]
pub struct ListRulesItem {
    pub id: String,
    pub name: String,
    pub status: Status,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CreateSource {
    pub r#type: String,
    pub name: String,
    pub format: String,
    pub conf: serde_json::Value,
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

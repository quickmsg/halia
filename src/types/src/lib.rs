use serde::{Deserialize, Serialize};

pub mod apps;
pub mod databoard;
pub mod devices;
pub mod events;
pub mod mqtt_server;
pub mod rules;
pub mod user;

#[derive(Serialize)]
pub struct Dashboard {
    pub machine_info: MachineInfo,
    pub device_summary: devices::Summary,
    pub app_summary: apps::Summary,
    pub databoard_summary: databoard::Summary,
    pub rule_summary: rules::Summary,
}

#[derive(Serialize)]
pub struct MachineInfo {
    pub start_time: u64,
    pub total_memory: u64,
    pub used_memory: u64,
    pub halia_memory: u64,
    pub global_cpu_usage: f32,
    pub disks: Vec<(String, u64, u64)>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct BaseConf {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Pagination {
    #[serde(rename = "p")]
    pub page: usize,
    #[serde(rename = "s")]
    pub size: usize,
}

impl Pagination {
    pub fn check(&self, index: usize) -> bool {
        index >= (self.page - 1) * self.size && index < self.page * self.size
    }

    pub fn to_sql(&self) -> (i64, i64) {
        ((self.size as i64), ((self.page - 1) * self.size) as i64)
    }
}

#[derive(Deserialize)]
pub struct Value {
    pub value: serde_json::Value,
}

#[derive(Serialize)]
pub struct RuleRef {
    pub rule_ref_cnt: usize,
    pub rule_active_ref_cnt: usize,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSourceOrSinkReq {
    pub base: BaseConf,
    pub ext: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct QuerySourcesOrSinksParams {
    pub name: Option<String>,
}

#[derive(Serialize)]
pub struct SearchSourcesOrSinksResp {
    pub total: usize,
    pub data: Vec<SearchSourcesOrSinksItemResp>,
}

#[derive(Serialize)]
pub struct SearchSourcesOrSinksItemResp {
    #[serde(flatten)]
    pub info: SearchSourcesOrSinksInfoResp,
    pub rule_ref: RuleRef,
}

#[derive(Serialize)]
pub struct SearchSourcesOrSinksInfoResp {
    pub id: String,
    pub conf: CreateUpdateSourceOrSinkReq,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct Ssl {
    pub enable: bool,
    pub verify: bool,
    pub alpn: Option<StringOrBytesValue>,
    pub ca_cert: Option<String>,
    pub client_cert: Option<String>,
    pub client_key: Option<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug)]
pub struct MessageRetain {
    #[serde(rename = "type")]
    pub typ: MessageRetainType,
    pub count: Option<usize>,
    pub time: Option<u64>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum MessageRetainType {
    All,
    None,
    LatestCount,
    LatestTime,
}

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    String,
    // base64 编码
    Bytes,
}

#[derive(Deserialize, Serialize, PartialEq, Clone, Debug)]
pub struct StringOrBytesValue {
    #[serde(rename = "type")]
    pub typ: ValueType,
    pub value: String,
}

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod apps;
pub mod databoard;
pub mod devices;
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
    pub id: Uuid,
    pub conf: CreateUpdateSourceOrSinkReq,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct CertInfo {
    pub ca_cert: String,
    pub client_cert: String,
    pub client_key: String,
    pub verify_server_cert: bool,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MessageRetain {
    #[serde(rename = "type")]
    pub typ: MessageRetainType,
    pub count: Option<usize>,
    pub time: Option<u64>,
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MessageRetainType {
    All,
    LatestCount,
    LatestTime,
}

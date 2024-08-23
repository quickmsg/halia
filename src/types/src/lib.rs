use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod apps;
pub mod devices;
pub mod rules;

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
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: serde_json::Value,
}

#[derive(Serialize)]
pub struct SearchSourcesOrSinksResp {
    pub total: usize,
    pub data: Vec<SearchSourcesOrSinksItemResp>,
}

#[derive(Serialize)]
pub struct SearchSourcesOrSinksItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSourceOrSinkReq,
    pub rule_ref: RuleRef,
}

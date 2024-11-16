use serde::{Deserialize, Serialize};

use crate::{devices::ConfType, Status};

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct CreateUpdateReq {
    pub name: String,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub conf: serde_json::Value,
}

#[derive(Serialize)]
pub struct ListSourcesSinksResp {
    pub count: usize,
    pub list: Vec<ListSourcesSinksItem>,
}

#[derive(Serialize)]
pub struct ListSourcesSinksItem {
    pub id: String,
    pub name: String,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
    pub rule_reference_running_cnt: usize,
    pub rule_reference_total_cnt: usize,
    pub can_delete: bool,
}

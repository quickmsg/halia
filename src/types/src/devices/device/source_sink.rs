use serde::{Deserialize, Serialize};

use crate::{devices::ConfType, RuleRef};

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct CreateUpdateReq {
    pub name: String,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub conf: serde_json::Value,
}

#[derive(Serialize)]
pub struct SearchResp {
    pub total: usize,
    pub data: Vec<SearchItemResp>,
}

#[derive(Serialize)]
pub struct SearchItemResp {
    pub id: String,
    pub req: CreateUpdateReq,
    pub rule_ref: RuleRef,
}

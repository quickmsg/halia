use serde::{Deserialize, Serialize};

use crate::{devices::ConfType, BaseConf, RuleRef};

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct CreateUpdateReq {
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub base: BaseConf,
    pub conf: serde_json::Value,
}

#[derive(Serialize)]
pub struct SearchResp {
    pub total: usize,
    pub data: Vec<SearchItemResp>,
}

#[derive(Serialize)]
pub struct SearchItemResp {
    pub req: CreateUpdateReq,
    pub rule_ref: RuleRef,
}
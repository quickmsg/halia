use serde::{Deserialize, Serialize};

use crate::devices::ConfType;

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct CreateUpdateReq {
    pub name: String,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub conf: serde_json::Value,
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
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
}

use serde::{Deserialize, Serialize};

use crate::BaseConf;

use super::Protocol;

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateReq {
    pub protocol: Protocol,
    pub base: BaseConf,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Clone)]
pub struct UpdateReq {
    pub base: BaseConf,
    pub conf: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub protocol: Option<Protocol>,
}

#[derive(Serialize)]
pub struct SearchResp {
    pub total: usize,
    pub data: Vec<SearchItemResp>,
}

#[derive(Serialize)]
pub struct SearchItemResp {
    pub id: String,
    pub req: CreateReq,
}

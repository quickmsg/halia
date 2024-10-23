use serde::{Deserialize, Serialize};

use crate::BaseConf;

use super::Protocol;

pub mod modbus;
pub mod source_sink;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct CreateReq {
    pub protocol: Protocol,
    pub base: BaseConf,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct UpdateReq {
    pub base: BaseConf,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
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

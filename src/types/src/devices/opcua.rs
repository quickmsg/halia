use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BaseConf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateOpcuaReq {
    #[serde(flatten)]
    pub base_conf: BaseConf,
    #[serde(flatten)]
    pub opcua_conf: OpcuaConf,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct OpcuaConf {
    pub host: String,
    pub port: u16,
    pub reconnect: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateVariableReq {
    #[serde(flatten)]
    pub base_conf: BaseConf,
    #[serde(flatten)]
    pub variable_conf: VariableConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct VariableConf {
    pub interval: u64,
}

#[derive(Serialize)]
pub struct SearchVariablesResp {
    pub total: usize,
    pub data: Vec<SearchVariablesItemResp>,
}

#[derive(Serialize)]
pub struct SearchVariablesItemResp {
    pub id: Uuid,
    #[serde(flatten)]
    pub conf: CreateUpdateVariableReq,
}

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

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct OpcuaConf {
    pub host: String,
    pub port: u16,
    pub reconnect: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateGroupReq {
    #[serde(flatten)]
    pub base_conf: BaseConf,
    #[serde(flatten)]
    pub group_conf: GroupConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct GroupConf {
    pub interval: u64,
    pub timestamps_to_return: TimestampsToReturn,
    pub max_age: f64,
}

#[derive(Serialize)]
pub struct SearchGroupsResp {
    pub total: usize,
    pub data: Vec<SearchGroupsItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupsItemResp {
    pub id: Uuid,
    #[serde(flatten)]
    pub conf: CreateUpdateGroupReq,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampsToReturn {
    Source = 0,
    Server = 1,
    Both = 2,
    Neither = 3,
    Invalid = 4,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateGroupVariableReq {
    #[serde(flatten)]
    pub base_conf: BaseConf,
    #[serde(flatten)]
    pub variable_conf: VariableConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct VariableConf {
    pub namespace: u16,
    pub identifier_type: IdentifierType,
    pub identifier: serde_json::Value,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierType {
    Numeric,
    String,
    Guid,
    ByteString,
}

#[derive(Serialize)]
pub struct SearchGroupVariablesResp {
    pub total: usize,
    pub data: Vec<SearchGroupVariablesItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupVariablesItemResp {
    pub id: Uuid,
    #[serde(flatten)]
    pub conf: CreateUpdateGroupVariableReq,
    pub value: serde_json::Value,
}

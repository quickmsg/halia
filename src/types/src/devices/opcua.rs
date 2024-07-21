use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BaseConf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateOpcuaReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateVariableReq {
    pub base: BaseConf,
    pub conf: VariableConf,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct VariableConf {

}

#[derive(Serialize)]
pub struct SearchVariablesResp {
    pub total: usize,
    pub data: Vec<SearchVariablesItemResp>,
}

#[derive(Serialize)]
pub struct SearchVariablesItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateVariableReq,
}
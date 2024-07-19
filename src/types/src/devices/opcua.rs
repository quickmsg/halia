use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateOpcuaReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize)]
pub struct CreateUpdateGroupReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
    pub interval: u64,
}

#[derive(Serialize)]
pub struct SearchGroupsResp {
    pub total: usize,
    pub data: Vec<SearchGroupsItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupsItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateGroupReq,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateGroupVariableReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
    // todo
    // pub node_id: NodeId,
}

#[derive(Serialize)]
pub struct SearchGroupVariablesResp {
    pub total: usize,
    pub data: Vec<SearchGroupVariablesItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupVariablesItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateGroupVariableReq,
}

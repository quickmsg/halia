use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateCoapReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateGroupReq {
    pub name: String,
    pub interval: u64,
    pub desc: Option<String>,
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
pub struct CreateUpdateGroupAPIReq {
    pub name: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,
    pub queries: Vec<(String, String)>,
    pub domain: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
}

#[derive(Serialize)]
pub struct SearchGroupAPIsResp {
    pub total: usize,
    pub data: Vec<SearchGroupAPIsItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupAPIsItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateGroupAPIReq,
}

#[derive(Deserialize, Serialize)]
pub struct CreateUpdateSinkReq {}

#[derive(Serialize)]
pub struct SearchSinksResp {
    pub total: usize,
    pub data: Vec<SearchSinksItemResp>,
}

#[derive(Serialize)]
pub struct SearchSinksItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
}
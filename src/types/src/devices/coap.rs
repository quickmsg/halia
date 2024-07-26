use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BaseConf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateCoapReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateAPIReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: APIConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct APIConf {
    // ms
    pub interval: u64,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,
    pub queries: Vec<(String, String)>,
    pub domain: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<Vec<u8>>,
}

#[derive(Serialize)]
pub struct SearchAPIsResp {
    pub total: usize,
    pub data: Vec<SearchAPIsItemResp>,
}

#[derive(Serialize)]
pub struct SearchAPIsItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateAPIReq,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSinkReq {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,

    pub method: String,
    pub path: String,
    pub data: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queries: Option<Vec<(String, String)>>,
    pub domain: String,
    pub token: Option<Vec<u8>>,
}

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

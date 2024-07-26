use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BaseConf, TargetValue};

#[derive(Deserialize, Serialize)]
pub struct CreateUpdateHttpClientReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: HttpClientConf,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct HttpClientConf {
    pub ssl: bool,
    pub host: String,
    pub port: u16,

    pub headers: HashMap<String, String>,
    // pub timeout: usize,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSinkReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: SinkConf,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    // GET POST DELETE PATCH
    pub method: String,
    pub path: String,
    pub params: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: TargetValue,
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

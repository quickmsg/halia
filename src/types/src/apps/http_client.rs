use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BaseConf, RuleRef};

#[derive(Deserialize, Serialize)]
pub struct CreateUpdateHttpClientReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: HttpClientConf,
}

// TODO 证书
#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct HttpClientConf {
    pub host: String,
    pub port: u16,

    pub ssl: bool,
    pub certifacte_verfication: bool,

    pub headers: Option<Vec<(String, String)>>,
    // 超时时间，单位为s
    // pub timeout: usize,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSourceReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: SourceConf,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SourceConf {
    pub method: SinkMethod,
    pub path: String,
    pub basic_auth: Option<BasicAuth>,
    pub headers: Vec<(String, String)>,
    pub query_params: Vec<(String, String)>,
    pub body: serde_json::Value,
}

#[derive(Serialize)]
pub struct SearchSourcesResp {
    pub total: usize,
    pub data: Vec<SearchSourcesItemResp>,
}

#[derive(Serialize)]
pub struct SearchSourcesItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSourceReq,
    pub rule_ref: RuleRef,
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
    pub method: SinkMethod,
    pub path: String,
    pub basic_auth: Option<BasicAuth>,
    pub headers: Vec<(String, String)>,
    pub query_params: Vec<(String, String)>,
    pub body: serde_json::Value,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SinkMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
    Head,
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
    pub rule_ref: RuleRef,
}

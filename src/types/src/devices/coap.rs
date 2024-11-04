use serde::{Deserialize, Serialize};

use crate::RuleRef;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateCoapReq {
    pub name: String,
    pub conf: CoapConf,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct CoapConf {
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateAPIReq {
    pub name: String,
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
    pub options: Vec<(CoapOption, String)>,
    pub domain: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<Vec<u8>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub enum CoapOption {
    IfMatch,
    UriHost,
    ETag,
    IfNoneMatch,
    Observe,
    UriPort,
    LocationPath,
    Oscore,
    UriPath,
    ContentFormat,
    MaxAge,
    UriQuery,
    Accept,
    LocationQuery,
    Block2,
    Block1,
    ProxyUri,
    ProxyScheme,
    Size1,
    Size2,
    NoResponse,
}

#[derive(Serialize)]
pub struct SearchAPIsResp {
    pub total: usize,
    pub data: Vec<SearchAPIsItemResp>,
}

#[derive(Serialize)]
pub struct SearchAPIsItemResp {
    pub id: String,
    pub conf: CreateUpdateAPIReq,
    #[serde(flatten)]
    pub rule_ref: RuleRef,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateObserveReq {
    pub name: String,
    #[serde(flatten)]
    pub ext: ObserveConf,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateSourceReq {
    pub name: String,
    #[serde(flatten)]
    pub ext: SourceConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct SourceConf {
    pub method: SourceMethod,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get: Option<GetConf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observe: Option<ObserveConf>,
    // #[serde(flatten)]
    // pub conf: serde_json::Value,
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub data: Option<Vec<u8>>,

    // pub domain: String,
    // #[serde(skip_serializing_if = "Option::is_none")]
    // pub token: Option<Vec<u8>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SourceMethod {
    Get,
    Observe,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct GetConf {
    pub interval: u64,
    pub path: String,
    pub querys: Vec<(String, String)>,
    pub options: Vec<(CoapOption, String)>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct ObserveConf {
    pub path: String,
    pub options: Vec<(CoapOption, String)>,
}

#[derive(Deserialize)]
pub struct QueryObserves {
    pub name: Option<String>,
}

#[derive(Serialize)]
pub struct SearchObservesResp {
    pub total: usize,
    pub data: Vec<SearchObservesItemResp>,
}

#[derive(Serialize)]
pub struct SearchObservesItemResp {
    pub id: String,
    pub conf: CreateUpdateObserveReq,
    pub rule_ref: RuleRef,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSinkReq {
    pub name: String,
    #[serde(flatten)]
    pub ext: SinkConf,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct SinkConf {
    pub method: SinkMethod,
    pub path: String,
    pub options: Vec<(CoapOption, String)>,
    pub data: Option<Vec<u8>>,
    pub domain: String,
    pub token: Option<Vec<u8>>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SinkMethod {
    Post,
    Put,
    Delete,
}

#[derive(Serialize)]
pub struct SearchSinksResp {
    pub total: usize,
    pub data: Vec<SearchSinksItemResp>,
}

#[derive(Serialize)]
pub struct SearchSinksItemResp {
    pub id: String,
    pub conf: CreateUpdateSinkReq,
    pub rule_ref: RuleRef,
}

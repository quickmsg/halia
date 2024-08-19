use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BaseConf, RuleRef};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateCoapReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: CoapConf,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct CoapConf {
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
    pub id: Uuid,
    pub conf: CreateUpdateAPIReq,
    #[serde(flatten)]
    pub rule_ref: RuleRef,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateObserveReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: ObserveConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct ObserveConf {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<u8>>,
    pub options: Vec<(CoapOption, String)>,
    pub domain: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<Vec<u8>>,
}

#[derive(Serialize)]
pub struct SearchObservesResp {
    pub total: usize,
    pub data: Vec<SearchObservesItemResp>,
}

#[derive(Serialize)]
pub struct SearchObservesItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateObserveReq,
    pub rule_ref: RuleRef,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSinkReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: SinkConf,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct SinkConf {
    pub method: SinkMethod,
    pub path: String,
    pub options: Vec<(CoapOption, String)>,
    pub data: Option<Vec<u8>>,
    pub domain: String,
    pub token: Option<Vec<u8>>,
}

#[derive(Deserialize, Serialize, Clone)]
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
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
}

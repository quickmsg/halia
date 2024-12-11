use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DeviceConf {
    pub host: String,
    pub port: u16,
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

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct SourceConf {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub querys: Option<Vec<(String, String)>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<(CoapOption, String)>>,

    pub method: SourceMethod,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get: Option<SourceGetConf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observe: Option<SourceObserveConf>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SourceMethod {
    Get,
    Observe,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct SourceGetConf {
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct SourceObserveConf {}

// #[derive(Serialize)]
// pub struct SearchObservesResp {
//     pub total: usize,
//     pub data: Vec<SearchObservesItemResp>,
// }

// #[derive(Serialize)]
// pub struct SearchObservesItemResp {
//     pub id: String,
//     pub conf: CreateUpdateObserveReq,
// }

// #[derive(Deserialize, Serialize, Clone)]
// pub struct CreateUpdateSinkReq {
//     pub name: String,
//     #[serde(flatten)]
//     pub ext: SinkConf,
// }

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

// #[derive(Serialize)]
// pub struct SearchSinksResp {
//     pub total: usize,
//     pub data: Vec<SearchSinksItemResp>,
// }

// #[derive(Serialize)]
// pub struct SearchSinksItemResp {
//     pub id: String,
//     pub conf: CreateUpdateSinkReq,
// }

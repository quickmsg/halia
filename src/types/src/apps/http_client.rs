use serde::{Deserialize, Serialize};

use crate::SslConf;

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct HttpClientConf {
    pub host: String,
    pub port: u16,

    pub basic_auth: Option<BasicAuth>,
    pub headers: Option<Vec<(String, String)>>,
    pub query_params: Option<Vec<(String, String)>>,
    // 超时时间，单位为s
    // pub timeout: usize,
    pub ssl_enable: bool,
    pub ssl_conf: Option<SslConf>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SourceConf {
    #[serde(rename = "type")]
    pub typ: SourceType,
    // 间隔 毫秒 当类型为http时必须存在
    pub interval: Option<u64>,
    pub path: String,
    pub basic_auth: Option<BasicAuth>,
    pub headers: Option<Vec<(String, String)>>,
    pub query_params: Option<Vec<(String, String)>>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Http,
    Websocket,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub method: SinkMethod,
    pub path: String,
    pub basic_auth: Option<BasicAuth>,
    pub headers: Option<Vec<(String, String)>>,
    pub query_params: Option<Vec<(String, String)>>,
    pub body: Option<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct BasicAuth {
    pub username: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
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

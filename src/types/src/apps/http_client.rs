use serde::{Deserialize, Serialize};

use crate::{SslConf, Status};

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct HttpClientConf {
    pub host: String,
    pub port: u16,

    pub basic_auth: Option<BasicAuth>,
    pub headers: Vec<(String, String)>,
    pub query_params: Vec<(String, String)>,
    // 超时时间，单位为s
    // pub timeout: usize,
    pub ssl_enable: bool,
    pub ssl_conf: Option<SslConf>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SourceConf {
    #[serde(rename = "type")]
    pub typ: SourceType,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub query_params: Vec<(String, String)>,
    pub http: Option<HttpSourceConf>,
    pub websocket: Option<WebsocketSourceConf>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Http,
    Websocket,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct HttpSourceConf {
    pub interval: u64,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct WebsocketSourceConf {
    pub reconnect: u64,
}

#[derive(Serialize)]
pub struct ListSourceConf {
    #[serde(rename = "type")]
    pub typ: SourceType,
    pub path: String,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub method: SinkMethod,
    pub path: String,
    pub headers: Vec<(String, serde_json::Value)>,
    pub query_params: Vec<(String, serde_json::Value)>,
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

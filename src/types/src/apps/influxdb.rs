use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct InfluxdbConf {
    pub host: String,
    pub port: u16,

    // pub ssl: bool,
    // pub certifacte_verfication: bool,
    pub headers: Option<Vec<(String, String)>>,
    pub query_params: Option<Vec<(String, String)>>,
    // 超时时间，单位为s
    // pub timeout: usize,
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

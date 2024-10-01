use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct InfluxdbConf {
    pub url: String,
    pub db: String,
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

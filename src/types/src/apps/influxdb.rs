use serde::{Deserialize, Serialize};

use crate::MessageRetain;

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct InfluxdbConf {
    pub url: String,
    pub org: String,
    pub api_token: String,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    // pub version: Version,
    pub bucket: String,
    pub message_retain: MessageRetain,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Version {
    V1,
    V2,
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

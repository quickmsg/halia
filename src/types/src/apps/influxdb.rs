use serde::{Deserialize, Serialize};

use crate::MessageRetain;

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct InfluxdbConf {
    pub version: InfluxdbVersion,
    #[serde(flatten)]
    pub v1: Option<InfluxdbV1>,
    #[serde(flatten)]
    pub v2: Option<InfluxdbV2>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum InfluxdbVersion {
    V1,
    V2,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct InfluxdbV1 {
    pub host: String,
    pub port: u16,
    pub database: String,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct InfluxdbV2 {
    pub url: String,
    pub port: u16,
    pub api_token: String,
    pub org: String,
    pub bucket: String,
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

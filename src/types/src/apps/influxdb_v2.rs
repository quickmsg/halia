use serde::{Deserialize, Serialize};

use crate::MessageRetain;

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct InfluxdbConf {
    pub url: String,
    pub port: u16,
    pub api_token: String,
    pub org: String,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub bucket: String,
    pub gizp: bool,
    pub mesaurement: String,
    pub fields: Vec<(String, serde_json::Value)>,
    pub tags: Option<Vec<(String, serde_json::Value)>>,
    pub precision: Precision,
    pub message_retain: MessageRetain,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub enum Precision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct BasicAuth {
    pub username: String,
    pub password: Option<String>,
}

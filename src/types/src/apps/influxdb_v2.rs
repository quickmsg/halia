use serde::{Deserialize, Serialize};

use crate::MessageRetain;

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct Conf {
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub org: String,
    pub bucket: String,
    pub api_token: String,
    pub mesaurement: String,
    pub fields: Vec<(String, serde_json::Value)>,
    pub tags: Option<Vec<(String, serde_json::Value)>>,
    pub precision: Precision,
    pub message_retain: MessageRetain,
    pub gzip: bool,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Precision {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

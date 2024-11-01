use serde::{Deserialize, Serialize};

use crate::{schema::{DecodeType, EncodeType}, MessageRetain, SslConf};

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct AppConf {
    pub host: String,
    pub port: u16,
    pub ssl_enable: bool,
    pub ssl_conf: Option<SslConf>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SourceConf {
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub decode_type: DecodeType,
    pub schema_id: Option<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub encode_type: EncodeType,
    pub schema_id: Option<String>,
    pub message_retain: MessageRetain,
}
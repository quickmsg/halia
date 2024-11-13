use serde::{Deserialize, Serialize};

use crate::{MessageRetain, SslConf};

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct InfluxdbConf {
    pub host: String,
    pub port: u16,

    pub auth_method: AuthMethod,
    pub auth_password: Option<AuthPassword>,
    pub auth_api_token: Option<AuthApiToken>,

    pub ssl_enable: bool,
    pub ssl_conf: Option<SslConf>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    None,
    Password,
    ApiToken,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct AuthPassword {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct AuthApiToken {
    pub api_token: String,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub database: String,
    pub gzip: bool,
    pub mesaurement: String,
    pub fields: Vec<(String, serde_json::Value)>,
    pub tags: Option<Vec<(String, serde_json::Value)>>,
    pub precision: Precision,
    pub message_retain: MessageRetain,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Precision {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
}

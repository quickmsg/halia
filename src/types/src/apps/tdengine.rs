use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct TDengineConf {
    pub host: String,
    pub port: u16,

    pub auth_method: TDengineAuthMethod,
    pub auth_password: Option<TDengineAuthPassword>,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub enum TDengineAuthMethod {
    None,
    Password,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct TDengineAuthPassword {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub db: String,
    pub table: String,
    pub values: Vec<String>,
}

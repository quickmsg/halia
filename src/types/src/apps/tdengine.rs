use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct TDengineConf {
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub db: String,
    pub table: String,
    pub values: Vec<(String, String)>,
}
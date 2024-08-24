use std::fmt;

use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::BaseConf;

pub mod http_client;
pub mod log;
pub mod mqtt_client;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct CreateUpdateAppReq {
    pub app_type: AppType,
    #[serde(flatten)]
    pub conf: AppConf,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AppType {
    MqttClient,
    HttpClient,
    Log,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct AppConf {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: serde_json::Value,
}

impl fmt::Display for AppType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AppType::MqttClient => write!(f, "mqtt_client"),
            AppType::HttpClient => write!(f, "http_client"),
            AppType::Log => write!(f, "log"),
        }
    }
}

impl TryFrom<&str> for AppType {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "mqtt_client" => Ok(AppType::MqttClient),
            "http_client" => Ok(AppType::HttpClient),
            "log" => Ok(AppType::Log),
            _ => bail!("未知应用类型: {}", value),
        }
    }
}

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
    pub running_cnt: usize,
    pub err_cnt: usize,
    pub off_cnt: usize,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub app_type: Option<AppType>,
    pub on: Option<bool>,
    pub err: Option<bool>,
}

#[derive(Serialize)]
pub struct SearchAppsResp {
    pub total: usize,
    pub data: Vec<SearchAppsItemResp>,
}

#[derive(Serialize)]
pub struct SearchAppsItemResp {
    pub id: Uuid,
    pub app_type: AppType,
    pub on: bool,
    pub err: Option<String>,
    pub rtt: u16,
    pub conf: SearchAppsItemConf,
}

#[derive(Serialize)]
pub struct SearchAppsItemConf {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: serde_json::Value,
}

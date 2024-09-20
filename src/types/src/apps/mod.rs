use std::fmt;

use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BaseConf, SearchSourcesOrSinksInfoResp};

pub mod http_client;
pub mod log;
pub mod mqtt_client;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct CreateUpdateAppReq {
    #[serde(rename = "type")]
    pub typ: AppType,
    #[serde(flatten)]
    pub conf: AppConf,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AppType {
    MqttClient,
    HttpClient,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct AppConf {
    pub base: BaseConf,
    pub ext: serde_json::Value,
}

impl fmt::Display for AppType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AppType::MqttClient => write!(f, "mqtt_client"),
            AppType::HttpClient => write!(f, "http_client"),
        }
    }
}

impl TryFrom<String> for AppType {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "mqtt_client" => Ok(AppType::MqttClient),
            "http_client" => Ok(AppType::HttpClient),
            _ => bail!("未知应用类型: {}", value),
        }
    }
}

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
    pub on: usize,
    pub running: usize,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub typ: Option<AppType>,
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
    pub common: SearchAppsItemCommon,
    pub conf: SearchAppsItemConf,
}

#[derive(Serialize)]
pub struct SearchAppsItemCommon {
    pub id: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub on: bool,
    pub source_cnt: usize,
    pub sink_cnt: usize,
    pub memory_info: Option<SearchAppsItemCommonMemory>,
}

#[derive(Serialize)]
pub struct SearchAppsItemCommonMemory {
    pub err: Option<String>,
    pub rtt: u16,
}

#[derive(Serialize)]
pub struct SearchAppsItemConf {
    pub base: BaseConf,
    pub ext: serde_json::Value,
}

#[derive(Deserialize)]
pub struct QueryRuleInfo {
    pub app_id: Uuid,
    pub source_id: Option<Uuid>,
    pub sink_id: Option<Uuid>,
}

#[derive(Serialize)]
pub struct SearchRuleInfo {
    pub app: SearchAppsItemResp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SearchSourcesOrSinksInfoResp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<SearchSourcesOrSinksInfoResp>,
}

use std::fmt;

use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};

use crate::{BaseConf, SearchSourcesOrSinksInfoResp};

pub mod http_client;
pub mod influxdb_v1;
pub mod influxdb_v2;
pub mod kafka;
pub mod mqtt_client_v311;
pub mod mqtt_client_v50;
pub mod tdengine;

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
    MqttClientV311,
    MqttClientV50,
    HttpClient,
    Kafka,
    InfluxdbV1,
    InfluxdbV2,
    Tdengine,
}

impl Into<i32> for AppType {
    fn into(self) -> i32 {
        match self {
            AppType::MqttClientV311 => 10,
            AppType::MqttClientV50 => 11,
            AppType::HttpClient => 2,
            AppType::Kafka => 3,
            AppType::InfluxdbV1 => 40,
            AppType::InfluxdbV2 => 41,
            AppType::Tdengine => 5,
        }
    }
}

impl TryFrom<i32> for AppType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            10 => Ok(AppType::MqttClientV311),
            11 => Ok(AppType::MqttClientV50),
            2 => Ok(AppType::HttpClient),
            3 => Ok(AppType::Kafka),
            40 => Ok(AppType::InfluxdbV1),
            41 => Ok(AppType::InfluxdbV2),
            5 => Ok(AppType::Tdengine),
            _ => bail!("未知应用类型: {}", value),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct AppConf {
    pub base: BaseConf,
    pub ext: serde_json::Value,
}

impl fmt::Display for AppType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AppType::MqttClientV311 => write!(f, "mqtt_client"),
            AppType::HttpClient => write!(f, "http_client"),
            AppType::Kafka => write!(f, "kafka"),
            AppType::InfluxdbV1 | AppType::InfluxdbV2 => write!(f, "influxdb"),
            AppType::Tdengine => write!(f, "tdengine"),
            AppType::MqttClientV50 => write!(f, "mqtt_client"),
        }
    }
}

// TODO
impl TryFrom<String> for AppType {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "mqtt_client" => Ok(AppType::MqttClientV311),
            "http_client" => Ok(AppType::HttpClient),
            "kafka" => Ok(AppType::Kafka),
            "influxdb" => Ok(AppType::InfluxdbV1),
            "tdengine" => Ok(AppType::Tdengine),
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
    pub typ: Option<String>,
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
    pub typ: AppType,
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
    pub app_id: String,
    pub source_id: Option<String>,
    pub sink_id: Option<String>,
}

#[derive(Serialize)]
pub struct SearchRuleInfo {
    pub app: SearchAppsItemResp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SearchSourcesOrSinksInfoResp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<SearchSourcesOrSinksInfoResp>,
}

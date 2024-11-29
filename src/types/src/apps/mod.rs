use std::{fmt, sync::Arc};

use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};

use crate::{RuleRefCnt, Status};

pub mod http_client;
pub mod influxdb_v1;
pub mod influxdb_v2;
pub mod kafka;
pub mod mqtt_client_v311;
pub mod mqtt_client_v50;
pub mod tdengine;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct CreateAppReq {
    pub app_type: AppType,
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct UpdateAppReq {
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AppType {
    MqttV311,
    MqttV50,
    Http,
    Kafka,
    InfluxdbV1,
    InfluxdbV2,
    Tdengine,
}

impl Into<i32> for AppType {
    fn into(self) -> i32 {
        match self {
            AppType::MqttV311 => 10,
            AppType::MqttV50 => 11,
            AppType::Http => 2,
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
            10 => Ok(AppType::MqttV311),
            11 => Ok(AppType::MqttV50),
            2 => Ok(AppType::Http),
            3 => Ok(AppType::Kafka),
            40 => Ok(AppType::InfluxdbV1),
            41 => Ok(AppType::InfluxdbV2),
            5 => Ok(AppType::Tdengine),
            _ => bail!("未知应用类型: {}", value),
        }
    }
}

impl fmt::Display for AppType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AppType::MqttV311 => write!(f, "mqtt_v311"),
            AppType::Http => write!(f, "http"),
            AppType::Kafka => write!(f, "kafka"),
            AppType::InfluxdbV1 | AppType::InfluxdbV2 => write!(f, "influxdb"),
            AppType::Tdengine => write!(f, "tdengine"),
            AppType::MqttV50 => write!(f, "mqtt_v50"),
        }
    }
}

impl TryFrom<String> for AppType {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "mqtt_v311" => Ok(AppType::MqttV311),
            "mqtt_v50" => Ok(AppType::MqttV50),
            "http" => Ok(AppType::Http),
            "kafka" => Ok(AppType::Kafka),
            "influxdb_v1" => Ok(AppType::InfluxdbV1),
            "influxdb_v2" => Ok(AppType::InfluxdbV2),
            "tdengine" => Ok(AppType::Tdengine),
            _ => bail!("未知应用类型: {}", value),
        }
    }
}

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub app_type: Option<String>,
    pub status: Option<Status>,
}

#[derive(Serialize)]
pub struct SearchAppsItemConf {
    pub name: String,
    pub ext: serde_json::Value,
}

#[derive(Deserialize)]
pub struct QueryRuleInfo {
    pub app_id: String,
    pub source_id: Option<String>,
    pub sink_id: Option<String>,
}

#[derive(Serialize)]
pub struct RuleInfoResp {
    pub app: RuleInfoApp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<RuleInfoSourceSink>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<RuleInfoSourceSink>,
}

#[derive(Serialize)]
pub struct RuleInfoApp {
    pub id: String,
    pub name: String,
    pub status: Status,
}

#[derive(Serialize)]
pub struct RuleInfoSourceSink {
    pub id: String,
    pub name: String,
    pub status: Status,
}

#[derive(Serialize)]
pub struct ListAppsResp {
    pub count: usize,
    pub list: Vec<ListAppsItem>,
}

#[derive(Serialize)]
pub struct ListAppsItem {
    pub id: String,
    pub app_type: AppType,
    pub name: String,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<Arc<String>>,
    #[serde(flatten)]
    pub rule_ref_cnt: RuleRefCnt,
    pub source_cnt: usize,
    pub sink_cnt: usize,
}

#[derive(Serialize)]
pub struct ReadAppResp {
    pub id: String,
    pub app_type: AppType,
    pub name: String,
    pub conf: serde_json::Value,
    pub status: Status,
    pub err: Option<Arc<String>>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSourceSinkReq {
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Deserialize)]
pub struct QuerySourcesSinksParams {
    pub name: Option<String>,
    pub status: Option<Status>,
}

#[derive(Serialize)]
pub struct ListSourcesSinksResp {
    pub count: usize,
    pub list: Vec<ListSourcesSinksItem>,
}

#[derive(Serialize)]
pub struct ListSourcesSinksItem {
    pub id: String,
    pub name: String,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<Arc<String>>,
    #[serde(flatten)]
    pub rule_ref_cnt: RuleRefCnt,

    #[serde(flatten)]
    pub conf: serde_json::Value,
}

#[derive(Serialize)]
pub struct ReadSourceSinkResp {
    pub id: String,
    pub name: String,
    pub conf: serde_json::Value,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<Arc<String>>,
    #[serde(flatten)]
    pub rule_ref_cnt: RuleRefCnt,
}

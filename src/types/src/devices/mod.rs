use std::{fmt, sync::Arc};

use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};

use crate::{RuleRefCnt, Status};

pub mod device;
pub mod device_template;
pub mod source_group;
pub mod source_sink_template;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    Modbus,
    Opcua,
    Coap,
}

impl Into<i32> for DeviceType {
    fn into(self) -> i32 {
        match self {
            DeviceType::Modbus => 1,
            DeviceType::Opcua => 2,
            DeviceType::Coap => 3,
        }
    }
}

impl TryFrom<i32> for DeviceType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(DeviceType::Modbus),
            2 => Ok(DeviceType::Opcua),
            3 => Ok(DeviceType::Coap),
            _ => bail!("未知协议类型: {}", value),
        }
    }
}

impl fmt::Display for DeviceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeviceType::Modbus => write!(f, "modbus"),
            DeviceType::Opcua => write!(f, "opcua"),
            DeviceType::Coap => write!(f, "coap"),
        }
    }
}

impl TryFrom<&str> for DeviceType {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "modbus" => Ok(DeviceType::Modbus),
            "opcua" => Ok(DeviceType::Opcua),
            "coap" => Ok(DeviceType::Coap),
            _ => bail!("未知协议类型: {}", value),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConfType {
    // 自定义
    Customize,
    // 模板
    Template,
}

impl Into<i32> for ConfType {
    fn into(self) -> i32 {
        match self {
            ConfType::Customize => 1,
            ConfType::Template => 2,
        }
    }
}

impl TryFrom<i32> for ConfType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ConfType::Customize),
            2 => Ok(ConfType::Template),
            _ => bail!("未知配置类型: {}", value),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub device_type: Option<DeviceType>,
    pub on: Option<bool>,
    pub err: Option<bool>,
}

#[derive(Serialize)]
pub struct ListDevicesResp {
    pub count: usize,
    pub list: Vec<ListDevicesItem>,
}

#[derive(Serialize)]
pub struct ListDevicesItem {
    pub id: String,
    pub device_type: DeviceType,
    pub conf_type: ConfType,
    pub name: String,
    pub addr: String,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<Arc<String>>,
    #[serde(flatten)]
    pub rule_ref_cnt: RuleRefCnt,
    pub source_cnt: usize,
    pub sink_cnt: usize,
}

#[derive(Serialize)]
pub struct ReadDeviceResp {
    pub id: String,
    pub device_type: DeviceType,
    pub name: String,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub conf: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_conf: Option<serde_json::Value>,
    pub status: Status,
    pub err: Option<Arc<String>>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateSourceSinkReq {
    pub name: String,
    pub conf_type: ConfType,
    pub template_id: Option<String>,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct UpdateSourceSinkReq {
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
    // pub conf_type: ConfType,
    // pub template_id: Option<String>,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
    #[serde(flatten)]
    pub rule_ref_cnt: RuleRefCnt,
    // #[serde(flatten)]
    // pub conf: serde_json::Value,
}

#[derive(Serialize)]
pub struct ReadSourceSinkResp {
    pub id: String,
    pub name: String,
    pub conf_type: ConfType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_id: Option<String>,
    pub conf: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_conf: Option<serde_json::Value>,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
    #[serde(flatten)]
    pub rule_ref_cnt: RuleRefCnt,
}

#[derive(Deserialize)]
pub struct QueryRuleInfoParams {
    pub device_id: String,
    pub source_id: Option<String>,
    pub sink_id: Option<String>,
}

#[derive(Serialize)]
pub struct RuleInfoResp {
    pub device: RuleInfoDevice,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<RuleInfoSourceSink>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<RuleInfoSourceSink>,
}

#[derive(Serialize)]
pub struct RuleInfoDevice {
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

#[derive(Deserialize, Serialize)]
pub struct CreateUpdateDeviceSourceGroupReq {
    pub name: String,
    pub conf: serde_json::Value,
}

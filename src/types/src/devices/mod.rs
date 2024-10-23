use std::fmt;

use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};

pub mod coap;
pub mod device;
pub mod device_template;
pub mod modbus;
pub mod opcua;
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
    // 模板
    Template,
    // 自定义
    Customize,
}

impl Into<i32> for ConfType {
    fn into(self) -> i32 {
        match self {
            ConfType::Template => 1,
            ConfType::Customize => 2,
        }
    }
}

impl TryFrom<i32> for ConfType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ConfType::Template),
            2 => Ok(ConfType::Customize),
            _ => bail!("未知配置类型: {}", value),
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
    pub device_type: Option<DeviceType>,
    pub on: Option<bool>,
    pub err: Option<bool>,
}

// #[derive(Serialize)]
// pub struct SearchDevicesResp {
//     pub total: usize,
//     pub data: Vec<SearchDevicesItemResp>,
// }

// #[derive(Serialize)]
// pub struct SearchDevicesItemResp {
//     pub common: SearchDevicesItemCommon,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub running_info: Option<SearchDevicesItemRunningInfo>,
//     pub conf: SearchDevicesItemConf,
// }

// #[derive(Serialize)]
// pub struct SearchDevicesItemCommon {
//     pub id: String,
//     #[serde(rename = "type")]
//     pub typ: DeviceType,
//     pub on: bool,
//     pub source_cnt: usize,
//     pub sink_cnt: usize,
// }

// #[derive(Serialize)]
// pub struct SearchDevicesItemRunningInfo {
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub err: Option<String>,
//     pub rtt: u16,
// }

// #[derive(Serialize)]
// pub struct SearchDevicesItemConf {
//     pub base: BaseConf,
//     pub ext: serde_json::Value,
// }

// #[derive(Deserialize)]
// pub struct QueryRuleInfo {
//     pub device_id: String,
//     pub source_id: Option<String>,
//     pub sink_id: Option<String>,
// }

// #[derive(Serialize)]
// pub struct SearchRuleInfo {
//     pub device: SearchDevicesItemResp,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub source: Option<SearchSourcesOrSinksInfoResp>,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub sink: Option<SearchSourcesOrSinksInfoResp>,
// }

// #[derive(Debug, Deserialize)]
// pub struct QuerySourceOrSinkTemplateParams {
//     pub name: Option<String>,
//     pub device_type: Option<DeviceType>,
// }

// #[derive(Serialize)]
// pub struct SearchSourcesOrSinkTemplatesResp {
//     pub total: usize,
//     pub data: Vec<SearchSourcesOrSinkTemplatesItemResp>,
// }

// #[derive(Serialize)]
// pub struct SearchSourcesOrSinkTemplatesItemResp {
//     pub id: String,
//     pub name: String,
//     #[serde(skip_serializing_if = "Option::is_none")]
//     pub desc: Option<String>,
//     pub device_type: DeviceType,
//     pub conf: serde_json::Value,
// }

// #[derive(Serialize)]
// pub struct SearchSourcesOrSinksResp {
//     pub total: usize,
//     pub data: Vec<SearchSourcesOrSinksItemResp>,
// }

// #[derive(Serialize)]
// pub struct SearchSourcesOrSinksItemResp {
//     #[serde(flatten)]
//     pub info: SearchSourcesOrSinksInfoResp,
//     pub rule_ref: RuleRef,
// }

// #[derive(Deserialize, Serialize, Clone)]
// pub struct CreateUpdateSourceOrSinkTemplateReq {
//     pub device_type: DeviceType,
//     pub base: BaseConf,
//     pub ext: serde_json::Value,
// }

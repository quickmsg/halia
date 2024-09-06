use core::fmt;

use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BaseConf, SearchSourcesOrSinksInfoResp};

pub mod coap;
pub mod modbus;
pub mod opcua;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct CreateUpdateDeviceReq {
    pub device_type: DeviceType,
    #[serde(flatten)]
    pub conf: DeviceConf,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DeviceType {
    Modbus,
    Opcua,
    Coap,
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct DeviceConf {
    pub base: BaseConf,
    pub ext: serde_json::Value,
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

#[derive(Serialize)]
pub struct Summary {
    pub total: usize,
    pub on: usize,
    pub running: usize,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub device_type: Option<DeviceType>,
    pub on: Option<bool>,
    pub err: Option<bool>,
}

#[derive(Serialize)]
pub struct SearchDevicesResp {
    pub total: usize,
    pub data: Vec<SearchDevicesItemResp>,
}

#[derive(Serialize)]
pub struct SearchDevicesItemResp {
    pub common: SearchDevicesItemCommon,
    pub conf: SearchDevicesItemConf,
    pub source_cnt: usize,
    pub sink_cnt: usize,
}

#[derive(Serialize)]
pub struct SearchDevicesItemCommon {
    pub id: Uuid,
    pub device_type: DeviceType,
    pub on: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rtt: Option<u16>,
}

#[derive(Serialize)]
pub struct SearchDevicesItemConf {
    pub base: BaseConf,
    pub ext: serde_json::Value,
}

#[derive(Deserialize)]
pub struct QueryRuleInfo {
    pub device_id: Uuid,
    pub source_id: Option<Uuid>,
    pub sink_id: Option<Uuid>,
}

#[derive(Serialize)]
pub struct SearchRuleInfo {
    pub device: SearchDevicesItemResp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<SearchSourcesOrSinksInfoResp>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink: Option<SearchSourcesOrSinksInfoResp>,
}

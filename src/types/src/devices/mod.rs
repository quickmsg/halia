use serde::Serialize;
use uuid::Uuid;

use crate::BaseConf;

pub mod coap;
pub mod modbus;
pub mod opcua;

#[derive(Serialize)]
pub struct SearchDevicesResp {
    pub total: usize,
    pub data: Vec<SearchDevicesItemResp>,
}

#[derive(Serialize)]
pub struct SearchDevicesItemResp {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub typ: &'static str,
    pub on: bool,
    pub err: Option<String>,
    pub rtt: u16,
    pub conf: SearchDevicesItemConf,
}

#[derive(Serialize)]
pub struct SearchDevicesItemConf {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: serde_json::Value,
}

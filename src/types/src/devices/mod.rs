use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod coap;
pub mod modbus;
pub mod opcua;

#[derive(Serialize)]
pub struct SearchDevicesResp {
    pub total: usize,
    pub err_cnt: usize,
    pub close_cnt: usize,
    pub data: Vec<SearchDevicesItemResp>,
}

#[derive(Serialize)]
pub struct SearchDevicesItemResp {
    pub id: Uuid,
    pub r#type: &'static str,
    pub on: bool,
    pub err: bool,
    pub rtt: u16,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct SinkValue {
    pub typ: SinkValueType,
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SinkValueType {
    Const,
    Variable,
}

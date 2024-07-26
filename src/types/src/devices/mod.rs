use serde::Serialize;
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
    pub err: Option<String>,
    pub rtt: u16,
    pub conf: serde_json::Value,
}

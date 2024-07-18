use serde::Serialize;
use uuid::Uuid;

pub mod datatype;
pub mod modbus;
pub mod coap;

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

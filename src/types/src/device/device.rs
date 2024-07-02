use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Deserialize, Debug, Serialize)]
pub struct CreateDeviceReq {
    pub r#type: String,
    pub name: String,
    pub conf: Value,
}

#[derive(Deserialize, Serialize)]
pub struct UpdateDeviceReq {
    pub name: String,
    pub conf: Value,
}

#[derive(Serialize)]
pub struct DeviceDetailResp {
    pub id: Uuid,
    pub r#type: &'static str,
    pub name: String,
    pub conf: Value,
}

#[derive(Serialize)]
pub struct SearchDeviceResp {
    pub total: usize,
    pub err_cnt: usize,
    pub close_cnt: usize,
    pub data: Vec<SearchDeviceItemResp>,
}

#[derive(Serialize)]
pub struct SearchDeviceItemResp {
    pub id: Uuid,
    pub name: String,
    pub r#type: &'static str,
    pub on: bool,
    pub err: bool,
    pub rtt: u16,
    pub conf: Value,
}

#[derive(Deserialize, Serialize, Clone, Copy, PartialEq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Client,
    Server,
}

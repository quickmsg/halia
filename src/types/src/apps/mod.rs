use serde::Serialize;
use uuid::Uuid;

pub mod http_client;
pub mod mqtt_client;

#[derive(Serialize)]
pub struct SearchAppsResp {
    pub total: usize,
    pub data: Vec<SearchAppsItemResp>,
}

#[derive(Serialize)]
pub struct SearchAppsItemResp {
    pub id: Uuid,
    #[serde(rename = "type")]
    pub typ: &'static str,
    pub on: bool,
    pub conf: serde_json::Value,
}

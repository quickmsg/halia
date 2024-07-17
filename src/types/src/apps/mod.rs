use serde::Serialize;
use uuid::Uuid;

pub mod mqtt_client_v311;

#[derive(Serialize)]
pub struct SearchAppsResp {
    pub total: usize,
    pub data: Vec<SearchAppsItemResp>,
}

#[derive(Serialize)]
pub struct SearchAppsItemResp {
    pub id: Uuid,
    pub r#type: &'static str,
    pub conf: serde_json::Value,
}
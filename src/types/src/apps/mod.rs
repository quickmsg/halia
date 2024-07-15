use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub mod mqtt_client;

#[derive(Deserialize, Debug, Serialize, Clone)]
pub struct CreateAppReq {
    pub r#type: String,
    pub name: String,
    pub conf: Value,
}

#[derive(Serialize)]
pub struct SearchAppsResp {
    pub total: usize,
    pub data: Vec<SearchAppItemResp>,
}

#[derive(Serialize)]
pub struct SearchAppItemResp {
    pub id: Uuid,
    pub r#type: &'static str,
    pub conf: Value,
}
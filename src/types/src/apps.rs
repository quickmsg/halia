use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Deserialize, Debug, Serialize, Clone)]
pub struct CreateAppReq {
    pub r#type: String,
    pub name: String,
    pub conf: Value,
}

#[derive(Serialize)]
pub struct SearchConnectorResp {
    pub total: usize,
    pub data: Vec<SearchConnectorItemResp>,
}

#[derive(Serialize)]
pub struct SearchConnectorItemResp {
    pub id: Uuid,
    pub r#type: &'static str,
    pub name: String,
    pub conf: Value,
}

#[derive(Serialize)]
pub struct SearchSourceResp {
    pub total: usize,
    pub data: Vec<Value>,
}

#[derive(Serialize)]
pub struct SearchSinkResp {
    pub total: usize,
    pub data: Vec<Value>,
}

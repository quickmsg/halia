use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Debug, Serialize, Clone)]
pub struct CreateConnectorReq {
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
    pub r#type: &'static str,
    pub name: String,
    pub conf: Value,
}

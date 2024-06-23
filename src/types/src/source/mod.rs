use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub mod mqtt;

#[derive(Deserialize, Debug, Serialize)]
pub struct CreateSourceReq {
    pub r#type: String,
    pub name: String,
    pub conf: Value,
}

#[derive(Serialize)]
pub struct SourceDetailResp {
    pub id: Uuid,
    pub r#type: &'static str,
    pub name: String,
    pub conf: Value,
}

#[derive(Serialize)]
pub struct ListSourceResp {
    pub id: Uuid,
    pub name: String,
    pub r#type: String,
}

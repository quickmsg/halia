use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct CreateSinkReq {
    pub r#type: String,
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UpdateSinkReq {
    pub id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReadSinkResp {
    pub r#type: String,
    pub name: String,
    pub conf: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ListSinkResp {
    pub name: String,
    pub r#type: String,
}

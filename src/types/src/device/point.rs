use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Serialize)]
pub struct ListPointResp {
    pub id: Uuid,
    pub name: String,
    pub address: u16,
    pub r#type: String,
    pub value: json::Value,
    pub describe: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreatePointReq {
    pub name: String,
    pub conf: Value,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WritePointValueReq {
    pub value: Value,
}

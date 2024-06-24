use message::value::MessageValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreatePointReq {
    pub name: String,
    pub conf: Value,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WritePointValueReq {
    pub value: Value,
}

#[derive(Deserialize, Serialize)]
pub struct SearchPointResp {
    pub total: usize,
    pub data: Vec<SearchPointItemResp>,
}

#[derive(Deserialize, Serialize)]
pub struct SearchPointItemResp {
    pub id: Uuid,
    pub name: String,
    pub conf: Value,
    pub value: MessageValue,
}

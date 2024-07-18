use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
pub struct CreateUpdateMqttClientReq {
    pub name: String,


    pub client_id: String,
    pub host: String,
    pub port: u16,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    pub version: Version,
    pub timeout: usize,
    pub keep_alive: usize,
    pub clean_session: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Version {
    V311,
    V50,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSourceReq {
    pub name: String,
    pub topic: String,
    pub qos: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
}

#[derive(Serialize)]
pub struct SearchSourcesResp {
    pub total: usize,
    pub data: Vec<SearchSourcesItemResp>,
}

#[derive(Serialize)]
pub struct SearchSourcesItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSourceReq,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSinkReq {
    pub name: String,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
}

#[derive(Serialize)]
pub struct SearchSinksResp {
    pub total: usize,
    pub data: Vec<SearchSinksItemResp>,
}

#[derive(Serialize)]
pub struct SearchSinksItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
}

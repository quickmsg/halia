use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct CreateUpdateMqttClientReq {
    pub name: String,
    pub id: String,
    pub timeout: usize,
    pub keep_alive: usize,
    pub clean_session: bool,
    pub host: String,
    pub port: u16,
    pub desc: Option<String>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSourceReq {
    pub name: String,
    pub topic: String,
    pub qos: u8,
    pub desc: Option<String>,
}

#[derive(Serialize)]
pub struct SearchSourcesResp {
    pub total: usize,
    pub data: Vec<SearchSourcesItemResp>,
}

#[derive(Serialize)]
pub struct SearchSourcesItemResp {
    pub conf: CreateUpdateSourceReq,
}

#[derive(Deserialize, Serialize)]
pub struct CreateUpdateSinkReq {
    pub topic: String,
    pub qos: u8,
}

#[derive(Serialize)]
pub struct SearchSinksResp {
    pub total: usize,
    pub data: Vec<SearchSinksItemResp>,
}

#[derive(Serialize)]
pub struct SearchSinksItemResp {
    pub conf: CreateUpdateSinkReq,
}

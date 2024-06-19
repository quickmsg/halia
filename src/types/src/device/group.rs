use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize, Debug)]
pub struct CreateGroupReq {
    pub name: String,
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct UpdateGroupReq {
    pub name: String,
    pub interval: u64,
}

#[derive(Serialize)]
pub struct SearchGroupResp {
    pub total: usize,
    pub data: Vec<SearchGroupItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupItemResp {
    pub id: Uuid,
    pub name: String,
    pub point_count: u8,
    pub interval: u64,
}

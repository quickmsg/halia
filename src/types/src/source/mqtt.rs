use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Debug, Serialize)]
pub struct TopicReq {
    pub topic: String,
    pub qos: u8,
}

pub struct TopicResp {
    pub id: Uuid,
    pub topic: String,
    pub qos: u8,
}

pub struct SearchTopicResp {
    pub total: usize,
    pub data: Vec<TopicResp>,
}

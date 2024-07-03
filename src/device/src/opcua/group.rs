use std::collections::HashMap;

use message::MessageBatch;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use super::point::Point;

#[derive(Debug)]
pub struct Group {
    pub id: Uuid,
    pub name: String,
    pub interval: u64,
    pub points: RwLock<HashMap<Uuid, Point>>,
    pub device_id: Uuid,
    pub tx: Option<broadcast::Sender<MessageBatch>>,
    pub ref_cnt: usize,
}
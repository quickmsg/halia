use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct Point {
    pub id: Uuid,
    pub conf: Conf,
    pub name: String,
    pub quantity: u16,
    pub value: Value,
}

#[derive(Debug, Deserialize, Serialize)]
struct Conf {
    pub namespace: usize,
}

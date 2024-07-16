use serde::Serialize;

pub mod apps;
pub mod devices;
pub mod rule;

#[derive(Serialize)]
pub struct SearchResp {
    pub total: usize,
    pub data: Vec<serde_json::Value>,
}

use serde::Serialize;

pub mod apps;
pub mod devices;
pub mod rules;

#[derive(Serialize)]
pub struct SearchResp {
    pub total: usize,
    pub data: Vec<serde_json::Value>,
}

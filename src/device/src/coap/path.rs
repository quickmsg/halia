use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
struct Path {
    id: Uuid,
    path: String,
    method: Method,
    data: Option<String>,
    timeout: Option<usize>,
    interval: usize,
}

#[derive(Deserialize, Serialize)]
enum Method {
    GET,
    POST,
    PUT,
    DELETE,
}

fn new(id: Uuid, conf: serde_json::Value) -> Result<Path> {
    todo!()
}

impl Path {
    pub fn run() {}
}
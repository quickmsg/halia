use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct Source {
    pub app_id: String,
    pub source_id: String,
}

#[derive(Deserialize, Serialize)]
pub struct Sink {
    pub app_id: String,
    pub sink_id: String,
}

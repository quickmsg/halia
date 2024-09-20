use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct SourcePoint {
    pub device_id: String,
    pub point_id: String,
}

#[derive(Deserialize, Serialize)]
pub struct Sink {
    pub device_id: String,
    pub sink_id: String,
}

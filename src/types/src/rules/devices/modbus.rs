use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
pub struct Source {
    pub device_id: Uuid,
    pub source_id: Uuid,
}

#[derive(Deserialize, Serialize)]
pub struct Sink {
    pub device_id: Uuid,
    pub sink_id: Uuid,
}

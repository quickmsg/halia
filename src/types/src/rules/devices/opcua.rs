use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct SourceGroup {
    pub device_id: String,
    pub group_id: String,
}

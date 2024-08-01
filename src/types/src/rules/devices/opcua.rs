use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
pub struct SourceGroup {
    pub device_id: Uuid,
    pub group_id: Uuid,
}

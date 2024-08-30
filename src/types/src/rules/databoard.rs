use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
pub struct DataboardData {
    pub databoard_id: Uuid,
    pub data_id: Uuid,
}

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct DataboardData {
    pub databoard_id: String,
    pub data_id: String,
}

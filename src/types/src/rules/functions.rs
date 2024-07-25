use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct WindowConf {
    #[serde(rename = "type")]
    pub typ: String,
    pub count: Option<u64>,
    // s
    pub interval: Option<u64>,
}

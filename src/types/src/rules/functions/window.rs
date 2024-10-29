use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct Conf {
    #[serde(rename = "type")]
    pub typ: String,
    pub count: Option<u64>,
    // s
    pub interval: Option<u64>,
}

pub struct TimeThmbling {
    // ms
    pub interval: u64,
}

pub struct TimeHopping {
    // ms
    pub interval: u64,
    // ms
    pub hopping: u64,
}

pub struct TimeSession {
    pub timeout: u64,
    pub max: u64,
}

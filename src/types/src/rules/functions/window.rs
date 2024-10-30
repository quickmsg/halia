use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct Conf {
    #[serde(rename = "type")]
    pub typ: Type,
    pub time_thmbling: Option<TimeThmbling>,
    pub time_hopping: Option<TimeHopping>,
    pub time_session: Option<TimeSession>,
    pub count: Option<Count>,
}

#[derive(Deserialize, Serialize, Debug)]
pub enum Type {
    TimeThmbling,
    TimeHopping,
    TimeSession,
    Count,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TimeThmbling {
    // ms
    pub interval: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct TimeHopping {
    // ms
    pub interval: u64,
    // ms
    pub hopping: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TimeSession {
    pub timeout: u64,
    pub max: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Count {
    pub count: u64,
}

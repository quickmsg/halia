use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub event_type: Option<EventType>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Start,
    Stop,
    Connect,
    DisConnect,
}

impl Into<i32> for EventType {
    fn into(self) -> i32 {
        match self {
            EventType::Start => 1,
            EventType::Stop => 2,
            EventType::Connect => 3,
            EventType::DisConnect => 4,
        }
    }
}

impl TryFrom<i32> for EventType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(EventType::Start),
            2 => Ok(EventType::Stop),
            3 => Ok(EventType::Connect),
            4 => Ok(EventType::DisConnect),
            _ => Err(()),
        }
    }
}

#[derive(Serialize)]
pub struct SearchEventsResp {
    pub total: usize,
    pub data: Vec<SearchEventsItemResp>,
}

#[derive(Serialize)]
pub struct SearchEventsItemResp {
    pub id: Uuid,
    pub name: String,
    pub event_type: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
}

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    pub source_type: Option<SourceType>,
    pub event_type: Option<EventType>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Device,
    App,
    Rule,
}

impl Into<i32> for SourceType {
    fn into(self) -> i32 {
        match self {
            SourceType::Device => 1,
            SourceType::App => 2,
            SourceType::Rule => 3,
        }
    }
}

impl TryFrom<i32> for SourceType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(SourceType::Device),
            2 => Ok(SourceType::App),
            3 => Ok(SourceType::Rule),
            _ => Err(()),
        }
    }
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
    pub id: String,
    pub name: String,
    pub source_type: SourceType,
    pub event_type: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
}

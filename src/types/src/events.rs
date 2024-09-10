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

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Start,
    Stop,
    Connect,
    DisConnect,
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
    pub source_type: SourceType,
    pub event_type: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
}

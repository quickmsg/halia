use std::convert::Infallible;

use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct QueryParams {
    pub typ: Option<EventType>,
    pub resource_type: Option<ResourceType>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceType {
    Device,
    App,
    Databoard,
    Rule,
}

impl Into<String> for ResourceType {
    fn into(self) -> String {
        match self {
            ResourceType::Device => "device".to_owned(),
            ResourceType::App => "app".to_owned(),
            ResourceType::Databoard => "databoard".to_owned(),
            ResourceType::Rule => "rule".to_owned(),
        }
    }
}

impl TryFrom<&str> for ResourceType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "device" => Ok(ResourceType::Device),
            "app" => Ok(ResourceType::App),
            "databoard" => Ok(ResourceType::Databoard),
            "rule" => Ok(ResourceType::Rule),
            _ => Err(value.to_owned()),
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

impl Into<String> for EventType {
    fn into(self) -> String {
        match self {
            EventType::Start => "start".to_owned(),
            EventType::Stop => "stop".to_owned(),
            EventType::Connect => "connect".to_owned(),
            EventType::DisConnect => "disconnect".to_owned(),
        }
    }
}

impl TryFrom<String> for EventType {
    type Error = ();

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "start" => Ok(EventType::Start),
            "stop" => Ok(EventType::Stop),
            "connect" => Ok(EventType::Connect),
            "disconnect" => Ok(EventType::DisConnect),
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
    pub event_type: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
}

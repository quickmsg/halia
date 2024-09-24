use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
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

impl Into<i32> for ResourceType {
    fn into(self) -> i32 {
        match self {
            ResourceType::Device => 1,
            ResourceType::App => 2,
            ResourceType::Databoard => 3,
            ResourceType::Rule => 4,
        }
    }
}

impl TryFrom<i32> for ResourceType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ResourceType::Device),
            2 => Ok(ResourceType::App),
            3 => Ok(ResourceType::Databoard),
            4 => Ok(ResourceType::Rule),
            _ => Err(()),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Create,
    Delete,
    Start,
    Stop,
    Connect,
    DisConnect,
}

impl Into<i32> for EventType {
    fn into(self) -> i32 {
        match self {
            EventType::Create => 1,
            EventType::Delete => 2,
            EventType::Start => 3,
            EventType::Stop => 4,
            EventType::Connect => 5,
            EventType::DisConnect => 6,
        }
    }
}

impl TryFrom<i32> for EventType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(EventType::Create),
            2 => Ok(EventType::Delete),
            3 => Ok(EventType::Start),
            4 => Ok(EventType::Stop),
            5 => Ok(EventType::Connect),
            6 => Ok(EventType::DisConnect),
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
    pub name: String,
    pub resource_type: ResourceType,
    #[serde(rename = "type")]
    pub typ: EventType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<String>,
}

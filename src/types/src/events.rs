use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub typ: Option<EventType>,
    pub resource_type: Option<ResourceType>,
    pub begin_ts: Option<i64>,
    pub end_ts: Option<i64>,
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
    Update,
    Delete,
    Start,
    Stop,
    ConnectSucceed,
    ConnectFailed,
}

impl Into<i32> for EventType {
    fn into(self) -> i32 {
        match self {
            EventType::Create => 1,
            EventType::Update => 2,
            EventType::Delete => 3,
            EventType::Start => 4,
            EventType::Stop => 5,
            EventType::ConnectSucceed => 6,
            EventType::ConnectFailed => 7,
        }
    }
}

impl TryFrom<i32> for EventType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(EventType::Create),
            2 => Ok(EventType::Update),
            3 => Ok(EventType::Delete),
            4 => Ok(EventType::Start),
            5 => Ok(EventType::Stop),
            6 => Ok(EventType::ConnectSucceed),
            7 => Ok(EventType::ConnectFailed),
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
    pub ts: i64,
}

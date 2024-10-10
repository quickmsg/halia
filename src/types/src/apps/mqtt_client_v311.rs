use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{MessageRetain, Ssl, StringOrBytesValue};

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientConf {
    pub client_id: String,
    pub host: String,
    pub port: u16,

    #[serde(flatten)]
    pub auth: MqttClientAuth,

    pub ssl: Ssl,

    pub timeout: usize,
    // 秒
    pub keep_alive: u64,
    pub clean_session: bool,

    pub last_will: Option<LastWill>,
}


#[derive(Deserialize, Serialize, PartialEq)]
pub struct LastWill {
    pub topic: String,
    pub qos: Qos,
    pub retain: bool,
    pub message: StringOrBytesValue,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientAuth {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct SourceConf {
    pub topic: String,
    pub qos: Qos,
}

#[derive(Deserialize_repr, Serialize_repr, Clone, PartialEq, Copy)]
#[repr(u8)]
pub enum Qos {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub topic: String,
    pub qos: Qos,
    pub retain: bool,
    pub message_retain: MessageRetain,
}

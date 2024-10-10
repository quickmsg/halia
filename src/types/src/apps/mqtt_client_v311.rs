use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{MessageRetain, Ssl, StringOrBytesValue};

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientConf {
    pub host: String,
    pub port: u16,

    pub client_id: String,

    pub auth_method: MqttClientAuthMethod,
    pub auth_password: Option<MqttClientAuthPassword>,

    pub timeout: usize,
    // 秒
    pub keep_alive: u64,
    pub clean_session: bool,

    pub ssl_enable: bool,
    pub ssl: Option<Ssl>,

    pub last_will_enable: bool,
    pub last_will: Option<LastWill>,
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MqttClientAuthMethod {
    None,
    Password,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct LastWill {
    pub topic: String,
    pub qos: Qos,
    pub retain: bool,
    pub message: StringOrBytesValue,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientAuthPassword {
    pub username: String,
    pub password: String,
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

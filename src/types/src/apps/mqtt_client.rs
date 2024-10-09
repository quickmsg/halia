use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{MessageRetain, Ssl, StringOrBytesValue};

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientConf {
    pub version: Version,
    #[serde(flatten)]
    pub v311: Option<MqttClientV311Conf>,
    #[serde(flatten)]
    pub v50: Option<MqttClientV50Conf>,
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Version {
    V311,
    V50,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientV311Conf {
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

    pub last_will: Option<LastWillV311>,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct LastWillV311 {
    pub topic: String,
    pub qos: Qos,
    pub retain: bool,
    pub message: StringOrBytesValue,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientV50Conf {
    pub client_id: String,
    pub host: String,
    pub port: u16,
    #[serde(flatten)]
    pub auth: MqttClientAuth,
    pub timeout: usize,
    pub keep_alive: u64,
    pub clean_start: bool,

    // s
    pub session_expire_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub max_packet_size: Option<u32>,
    pub topic_alias_max: Option<u16>,
    pub request_response_info: Option<u8>,
    pub request_problem_info: Option<u8>,
    pub user_properties: Option<Vec<(String, String)>>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<StringOrBytesValue>,

    pub ssl: Ssl,
    pub last_will: Option<LastWillV50>,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientAuth {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct LastWillV50 {
    pub topic: String,
    // base64 编码的信息
    pub message: String,
    pub qos: Qos,
    pub retain: bool,

    pub delay_interval: Option<u32>,
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    // base 64 编码
    pub correlation_data: Option<String>,
    pub user_properties: Vec<(String, String)>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct SourceConf {
    pub topic: String,
    pub qos: Qos,

    // v5
    // 订阅标识符
    pub subscribe_id: Option<usize>,
    // topic别名
    pub topic_alias: Option<u16>,
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

    // for v5
    // 1 开启 0关闭
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_properties: Option<Vec<(String, String)>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_format_indicator: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_expiry_interval: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_alias: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_data: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_identifiers: Option<Vec<usize>>,

    pub message_retain: MessageRetain,
}

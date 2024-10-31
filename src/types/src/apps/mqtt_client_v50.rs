use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use crate::{MessageRetain, PlainOrBase64Value, SslConf};

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientConf {
    pub client_id: String,
    pub host: String,
    pub port: u16,
    #[serde(flatten)]
    pub auth: MqttClientAuth,
    pub timeout: usize,
    pub keep_alive: u64,
    pub clean_start: bool,

    pub connect_properties: Option<ConnectProperties>,

    pub ssl_enable: bool,
    pub ssl_conf: Option<SslConf>,
    pub last_will: Option<LastWill>,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct ConnectProperties {
    pub session_expire_interval: Option<u32>,
    pub receive_maximum: Option<u16>,
    pub max_packet_size: Option<u32>,
    pub topic_alias_max: Option<u16>,
    pub request_response_info: Option<u8>,
    pub request_problem_info: Option<u8>,
    pub user_properties: Vec<(String, String)>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<PlainOrBase64Value>,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientAuth {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct LastWill {
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

    pub properties: Option<PublishProperties>,

    pub message_retain: MessageRetain,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct PublishProperties {
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
    pub correlation_data: Option<PlainOrBase64Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_identifiers: Option<Vec<usize>>,
}
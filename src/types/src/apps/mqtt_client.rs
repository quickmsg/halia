use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;

use crate::{BaseConf, RuleRef};

#[derive(Deserialize, Serialize)]
pub struct CreateUpdateMqttClientReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: MqttClientConf,
}

#[derive(Deserialize, Serialize, PartialEq)]
pub struct MqttClientConf {
    pub client_id: String,
    pub host: String,
    pub port: u16,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    // pub ca: Option<String>,
    // pub client_cert: Option<String>,
    // pub client_key: Option<String>,
    pub version: Version,
    pub timeout: usize,
    pub keep_alive: u64,
    pub clean_session: bool,
}

pub struct CertConf {
    pub ca: Bytes,
    pub client_cert: Bytes,
    pub client_key: Bytes,
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Version {
    V311,
    V50,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSourceReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: SourceConf,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct SourceConf {
    pub topic: String,
    pub qos: Qos,
}

#[derive(Deserialize_repr, Serialize_repr, Clone, PartialEq)]
#[repr(u8)]
pub enum Qos {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[derive(Serialize)]
pub struct SearchSourcesResp {
    pub total: usize,
    pub data: Vec<SearchSourcesItemResp>,
}

#[derive(Serialize)]
pub struct SearchSourcesItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSourceReq,
    #[serde(flatten)]
    pub rule_ref: RuleRef,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSinkReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: SinkConf,
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
}

#[derive(Serialize)]
pub struct SearchSinksResp {
    pub total: usize,
    pub data: Vec<SearchSinksItemResp>,
}

#[derive(Serialize)]
pub struct SearchSinksItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
    #[serde(flatten)]
    pub rule_ref: RuleRef,
}

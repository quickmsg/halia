use serde::{Deserialize, Serialize};

use crate::{MessageRetain, PlainOrBase64Value};

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct Conf {
    pub bootstrap_brokers: Vec<(String, u16)>,

    // pub ssl: bool,
    // pub certifacte_verfication: bool,
    pub reconnect: u64,
    // 超时时间，单位为s
    // pub timeout: usize,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
pub struct SinkConf {
    pub topic: String,
    pub partition: i32,
    pub key: Option<PlainOrBase64Value>,
    pub headers: Option<Vec<(String, PlainOrBase64Value)>>,
    pub unknown_topic_handling: UnknownTopicHandling,
    pub compression: Compression,
    // ms
    pub ack_timeout: u64,
    pub required_acks: RequiredAcks,
    pub message_retain: MessageRetain,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum UnknownTopicHandling {
    Error,
    Retry,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum RequiredAcks {
    None,
    One,
    All,
}

#[derive(Deserialize, Serialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    None,
    Gzip,
    Lz4,
    Snappy,
    Zstd,
}

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct OpcuaConf {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub reconnect: u64, // 秒
    pub auth_method: AuthMethod,
    pub auth_username: Option<AuthUsername>,
    pub auth_certificate: Option<AuthCertificate>,

    // pub use_security: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum AuthMethod {
    Anonymous,
    Username,
    X509,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct AuthUsername {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct AuthCertificate {
    pub cert: String,
    pub key: String,
}

// TODO
pub struct IssuedToken {
    pub token_type: String,
    pub security_policy_uri: String,
    pub issuer_endpoint_url: String,
    pub security_mode: String,
    pub token: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct SourceConf {
    #[serde(rename = "type")]
    pub typ: SourceType,
    pub group: Option<GroupConf>,
    pub subscription: Option<Subscriptionconf>,
    pub monitored_item: Option<MonitoredItemconf>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Group,
    Subscription,
    MonitoredItem,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct GroupConf {
    // 毫秒
    pub interval: u64,
    // pub max_age: f64,
    // serde_json bug,无法在flatten模式下解析f64
    pub max_age: f64,
    pub variables: Vec<VariableConf>,
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampsToReturn {
    Source,
    Server,
    Both,
    Neither,
    Invalid,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct VariableConf {
    pub field: String,
    pub node_id: NodeId,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct NodeId {
    pub namespace: u16,
    pub identifier: Identifer,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Identifer {
    pub typ: IdentifierType,
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierType {
    Numeric,
    String,
    Guid,
    Opaque,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Subscriptionconf {
    // 秒
    pub publishing_interval: u64,

    pub lifetime_count: u32,
    pub max_keep_alive_count: u32,
    pub max_notifications_per_publish: u32,
    pub priority: u8,
    pub publishing_enabled: bool,

    pub monitored_items: Vec<MonitoredItemconf>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct MonitoredItemconf {
    #[serde(flatten)]
    pub variable: VariableConf,
    pub attribute_id: u32,
    pub index_range: String,
    pub data_encoding: String,
    pub monitoring_mode: MonitoringMode,

    pub client_handle: u32,
    pub sampling_interval: f64,
    // pub filter:
    pub queue_size: u32,
    pub discard_oldest: bool,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MonitoringMode {
    Disabled,
    Sampling,
    Reporting,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct SinkConf {
    pub field: String,
    pub node_id: NodeId,
}

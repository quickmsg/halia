use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{BaseConf, RuleRef};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateOpcuaReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: OpcuaConf,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct OpcuaConf {
    pub host: String,
    pub port: u16,
    pub reconnect: u64,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateGroupReq {
    #[serde(flatten)]
    pub base_conf: BaseConf,
    #[serde(flatten)]
    pub group_conf: GroupConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct GroupConf {
    pub interval: u64,
    pub timestamps_to_return: TimestampsToReturn,
    // pub max_age: f64,
    // serde_json bug,无法在flatten模式下解析f64
    pub max_age: i64,
}

#[derive(Serialize)]
pub struct SearchGroupsResp {
    pub total: usize,
    pub data: Vec<SearchGroupsItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupsItemResp {
    pub id: Uuid,
    #[serde(flatten)]
    pub conf: CreateUpdateGroupReq,
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

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateGroupVariableReq {
    #[serde(flatten)]
    pub base_conf: BaseConf,
    #[serde(flatten)]
    pub variable_conf: VariableConf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct VariableConf {
    pub namespace: u16,
    pub identifier_type: IdentifierType,
    pub identifier: serde_json::Value,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct CreateUpdateSubscriptionReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: Subscriptionconf,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Subscriptionconf {
    // 秒
    pub publishing_interval: u64,

    pub lifetime_count: u32,
    pub max_keep_alive_count: u32,
    pub max_notifications_per_publish: u32,
    pub priority: u8,
    pub publishing_enalbed: bool,

    pub monitored_items: Vec<VariableConf>,
}

#[derive(Serialize)]
pub struct SearchSubscriptionsResp {
    pub total: usize,
    pub data: Vec<SearchSubscriptionsItemResp>,
}

#[derive(Serialize)]
pub struct SearchSubscriptionsItemResp {
    pub id: Uuid,
    #[serde(flatten)]
    pub conf: CreateUpdateSubscriptionReq,
    pub rule_ref: RuleRef,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum IdentifierType {
    Numeric,
    String,
    Guid,
    ByteString,
}

#[derive(Serialize)]
pub struct SearchGroupVariablesResp {
    pub total: usize,
    pub data: Vec<SearchGroupVariablesItemResp>,
}

#[derive(Serialize)]
pub struct SearchGroupVariablesItemResp {
    pub id: Uuid,
    #[serde(flatten)]
    pub conf: CreateUpdateGroupVariableReq,
    pub value: serde_json::Value,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateEventReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: EventConf,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct EventConf {
    // ms
    pub publishing_interval: u64,

    pub lifetime_count: u32,
    pub max_keep_alive_count: u32,
    pub max_notifications_per_publish: u32,
    pub priority: u8,
    pub publishing_enabled: bool,
    // todo
}

#[derive(Serialize)]
pub struct SearchEventsResp {
    pub total: usize,
    pub data: Vec<SearchEventsItemResp>,
}

#[derive(Serialize)]
pub struct SearchEventsItemResp {
    pub id: Uuid,
    #[serde(flatten)]
    pub conf: CreateUpdateEventReq,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CreateUpdateSinkReq {
    #[serde(flatten)]
    pub base: BaseConf,
    #[serde(flatten)]
    pub ext: SinkConf,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct SinkConf {}

#[derive(Serialize)]
pub struct SearchSinksResp {
    pub total: usize,
    pub data: Vec<SearchSinksItemResp>,
}

#[derive(Serialize, Clone)]
pub struct SearchSinksItemResp {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,
}

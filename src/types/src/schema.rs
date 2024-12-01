use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateSchemaReq {
    pub name: String,
    pub schema_type: SchemaType,
    pub protocol_type: ProtocolType,
    pub conf: serde_json::Value,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SchemaType {
    Encode,
    Decode,
}

impl Into<i32> for SchemaType {
    fn into(self) -> i32 {
        match self {
            SchemaType::Encode => 1,
            SchemaType::Decode => 2,
        }
    }
}

impl TryFrom<i32> for SchemaType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(SchemaType::Encode),
            2 => Ok(SchemaType::Decode),
            _ => bail!("未知模式类型: {}", value),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DecodeType {
    Raw,
    Json,
    Csv,
    Avro,
    Yaml,
    Toml,
    Protobuf,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum EncodeType {
    Template,
    Json,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolType {
    Avro,
    Protobuf,
    Csv,
    Template,
}

impl Into<i32> for ProtocolType {
    fn into(self) -> i32 {
        match self {
            ProtocolType::Avro => 1,
            ProtocolType::Protobuf => 2,
            ProtocolType::Csv => 3,
            ProtocolType::Template => 4,
        }
    }
}

impl TryFrom<i32> for ProtocolType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ProtocolType::Avro),
            2 => Ok(ProtocolType::Protobuf),
            3 => Ok(ProtocolType::Csv),
            4 => Ok(ProtocolType::Template),
            _ => bail!("未知模式类型: {}", value),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub name: Option<String>,
    #[serde(rename = "type")]
    pub typ: Option<SchemaType>,
    pub device_type: Option<ProtocolType>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AvroDecodeConf {
    // pub base64_decode: bool,
    pub schema: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ProtobufDecodeConf {
    // pub base64_decode: bool,
    pub descriptor: String,
    pub message_type: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CsvDecodeConf {
    // pub base64_decode: bool,
    pub has_headers: bool,
    pub headers: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TemplateEncodeConf {
    pub template: String,
}

#[derive(Serialize)]
pub struct ListSchemasResp {
    pub count: usize,
    pub list: Vec<ListSchemasItem>,
}

#[derive(Serialize)]
pub struct ListSchemasItem {
    pub id: String,
    pub name: String,
    pub schema_type: SchemaType,
    pub protocol_type: ProtocolType,
    pub reference_cnt: usize,
}

#[derive(Serialize)]
pub struct ReadSchemaResp {
    pub id: String,
    pub name: String,
    pub schema_type: SchemaType,
    pub protocol_type: ProtocolType,
    pub conf: serde_json::Value,
    pub reference_cnt: usize,
    pub can_delete: bool,
}

#[derive(Serialize)]
pub struct ListReferencesResp {
    pub count: usize,
    pub list: Vec<ListReferencesItem>,
}

#[derive(Serialize)]
pub struct ListReferencesItem {
    pub parent_type: ParentType,
    pub parent_id: String,
    pub parent_name: String,
    pub resource_type: ResourceType,
    pub resource_id: String,
    pub resource_name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParentType {
    App,
    Device,
}

impl Into<i32> for ParentType {
    fn into(self) -> i32 {
        match self {
            ParentType::App => 1,
            ParentType::Device => 2,
        }
    }
}

impl TryFrom<i32> for ParentType {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ParentType::App),
            2 => Ok(ParentType::Device),
            _ => bail!("未知资源类型: {}", value),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceType {
    Source,
    Sink,
}

impl Into<i32> for ResourceType {
    fn into(self) -> i32 {
        match self {
            ResourceType::Source => 1,
            ResourceType::Sink => 2,
        }
    }
}

impl TryFrom<i32> for ResourceType {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(ResourceType::Source),
            2 => Ok(ResourceType::Sink),
            _ => bail!("未知资源类型: {}", value),
        }
    }
}

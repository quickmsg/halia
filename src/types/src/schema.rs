use anyhow::{bail, Error};
use serde::{Deserialize, Serialize};

use crate::BaseConf;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CreateUpdateSchemaReq {
    #[serde(rename = "type")]
    pub typ: SchemaType,
    pub protocol: ProtocolType,
    pub base: BaseConf,
    pub ext: serde_json::Value,
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
    CsvWithSchema,
    Avro,
    AvroWithSchema,
    Yaml,
    Toml,
    Protobuf,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub enum EncodeType {
    Template,
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
    pub protocol: Option<ProtocolType>,
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
pub struct SearchSchemasResp {
    pub total: usize,
    pub data: Vec<SearchSchemasItemResp>,
}

#[derive(Serialize)]
pub struct SearchSchemasItemResp {
    pub conf: CreateUpdateSchemaReq,
}

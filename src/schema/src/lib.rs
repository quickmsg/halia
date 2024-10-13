use anyhow::Result;
use bytes::Bytes;
use message::MessageBatch;

pub mod avro;
pub mod avro_no_schema;
pub mod csv;
pub mod json;
pub mod yaml;

pub enum Schema {
    Json,
    Csv,
    Avro,
    Yaml,
    Toml,
}

pub trait Coder {
    fn decode(&self, data: Bytes) -> Result<MessageBatch>;
    fn encode(&self, mb: MessageBatch) -> Result<Bytes>;
}

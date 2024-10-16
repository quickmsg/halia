use anyhow::Result;
use bytes::Bytes;
use message::MessageBatch;

pub mod avro;
pub mod csv;
pub mod json;
pub mod yaml;
pub mod decoders;

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

pub trait Decoder {
    fn decode(&self, data: Bytes) -> Result<MessageBatch>;
}

pub trait Encoder {
    fn encode(&self, mb: MessageBatch) -> Result<Bytes>;
}
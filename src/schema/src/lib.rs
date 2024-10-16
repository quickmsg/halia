use anyhow::Result;
use bytes::Bytes;
use message::MessageBatch;

pub mod decoders;
pub mod encoders;

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
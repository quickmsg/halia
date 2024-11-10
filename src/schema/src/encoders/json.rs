use bytes::Bytes;
use common::error::HaliaResult;

use crate::Encoder;

struct Json;

pub fn new() -> HaliaResult<Box<dyn Encoder>> {
    Ok(Box::new(Json))
}

impl Encoder for Json {
    fn encode(&self, mb: message::MessageBatch) -> anyhow::Result<bytes::Bytes> {
        Ok(Bytes::from(mb.to_json()))
    }
}

use anyhow::Result;
use bytes::Bytes;
use common::error::HaliaResult;
use message::MessageBatch;

use crate::Encoder;

pub(crate) mod template;

struct Template {
    template: String,
}

// TODO
pub(crate) fn new() -> HaliaResult<Box<dyn Encoder>> {
    Ok(Box::new(Template {
        template: "".to_string(),
    }))
}

impl Encoder for Template {
    fn encode(&self, _mb: MessageBatch) -> Result<Bytes> {
        unimplemented!()
    }
}
use common::error::HaliaResult;
use message::{Message, MessageBatch};

use crate::Decoder;

struct Raw;

pub fn new() -> HaliaResult<Box<dyn Decoder>> {
    Ok(Box::new(Raw))
}

impl Decoder for Raw {
    fn decode(&self, data: bytes::Bytes) -> anyhow::Result<message::MessageBatch> {
        let mut message = Message::default();
        message.add(
            "raw_data".to_owned(),
            message::MessageValue::Bytes(data.to_vec()),
        );
        let mut mb = MessageBatch::default();
        mb.push_message(message);
        Ok(mb)
    }
}

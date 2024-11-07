use anyhow::Result;
use lz4_flex::{compress, decompress};
use message::MessageValue;
use tracing::warn;

use super::Compresser;

struct HaliaLz4Encoder {
    field: String,
    target_field: String,
}

pub fn new_encoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaLz4Encoder {
        field,
        target_field,
    })
}

impl HaliaLz4Encoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        Ok(compress(bytes))
    }
}

impl Compresser for HaliaLz4Encoder {
    fn code(&mut self, mb: &mut message::MessageBatch) {
        for message in mb.get_messages_mut() {
            match message.get(&self.field) {
                Some(mv) => match mv {
                    message::MessageValue::String(str) => match Self::encode(str.as_bytes()) {
                        Ok(data) => message.add(self.field.clone(), MessageValue::Bytes(data)),
                        Err(e) => warn!("{}", e),
                    },
                    message::MessageValue::Bytes(bytes) => match Self::encode(bytes) {
                        Ok(data) => message.add(self.field.clone(), MessageValue::Bytes(data)),
                        Err(e) => warn!("{}", e),
                    },
                    _ => {}
                },
                None => {}
            }
        }
    }
}

struct HaliaLz4Decoder {
    field: String,
    target_field: String,
}

pub fn new_decoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaLz4Decoder {
        field,
        target_field,
    })
}

impl HaliaLz4Decoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let data = decompress(bytes, bytes.len() * 10)?;
        Ok(data)
    }
}

impl Compresser for HaliaLz4Decoder {
    fn code(&mut self, mb: &mut message::MessageBatch) {
        for message in mb.get_messages_mut() {
            match message.get(&self.field) {
                Some(mv) => match mv {
                    message::MessageValue::String(str) => match Self::decode(str.as_bytes()) {
                        Ok(data) => message.add(self.field.clone(), MessageValue::Bytes(data)),
                        Err(e) => warn!("{}", e),
                    },
                    message::MessageValue::Bytes(bytes) => match Self::decode(bytes) {
                        Ok(data) => message.add(self.field.clone(), MessageValue::Bytes(data)),
                        Err(e) => warn!("{}", e),
                    },
                    _ => {}
                },
                None => {}
            }
        }
    }
}

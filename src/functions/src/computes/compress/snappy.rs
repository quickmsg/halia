use anyhow::Result;
use message::MessageValue;
use snap::raw::{Decoder, Encoder};
use tracing::warn;

use super::Compresser;

struct HaliaSnappyEncoder {
    field: String,
    target_field: String,
}

pub fn new_encoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaSnappyEncoder {
        field,
        target_field,
    })
}

impl HaliaSnappyEncoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut snappyer = Encoder::new();
        let data = snappyer.compress_vec(bytes)?;
        Ok(data)
    }
}

impl Compresser for HaliaSnappyEncoder {
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

struct HaliaSnappyDecoder {
    field: String,
    target_field: String,
}

pub fn new_decoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaSnappyDecoder {
        field,
        target_field,
    })
}

impl HaliaSnappyDecoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut snappyer = Decoder::new();
        let data = snappyer.decompress_vec(bytes)?;
        Ok(data)
    }
}

impl Compresser for HaliaSnappyDecoder {
    fn code(&mut self, mb: &mut message::MessageBatch) {
        for message in mb.get_messages_mut() {
            match message.get(&self.field) {
                Some(mv) => match mv {
                    message::MessageValue::String(str) => match Self::decode(str.as_bytes()) {
                        Ok(data) => message.add(self.field.clone(), MessageValue::Bytes(data)),
                        Err(e) => warn!("decode err {}", e),
                    },
                    message::MessageValue::Bytes(bytes) => match Self::decode(bytes) {
                        Ok(data) => message.add(self.field.clone(), MessageValue::Bytes(data)),
                        Err(e) => warn!("decode err {}", e),
                    },
                    _ => {}
                },
                None => {}
            }
        }
    }
}

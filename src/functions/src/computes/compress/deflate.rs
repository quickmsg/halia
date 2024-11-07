use std::io::Write as _;

use anyhow::Result;
use flate2::{
    write::{DeflateDecoder, DeflateEncoder},
    Compression,
};
use message::MessageValue;
use tracing::warn;

use super::Compresser;

struct HaliaDeflateEncoder {
    field: String,
    target_field: String,
}

pub fn new_encoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaDeflateEncoder {
        field,
        target_field,
    })
}

impl HaliaDeflateEncoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut deflater = DeflateEncoder::new(vec![], Compression::default());
        deflater.write_all(bytes)?;
        let data = deflater.finish()?;
        Ok(data)
    }
}

impl Compresser for HaliaDeflateEncoder {
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

struct HaliaDeflateDecoder {
    field: String,
    target_field: String,
}

pub fn new_decoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaDeflateDecoder {
        field,
        target_field,
    })
}

impl HaliaDeflateDecoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut deflater = DeflateDecoder::new(vec![]);
        deflater.write_all(bytes)?;
        let data = deflater.finish()?;
        Ok(data)
    }
}

impl Compresser for HaliaDeflateDecoder {
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

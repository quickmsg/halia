use std::io::Write as _;

use anyhow::Result;
use flate2::{
    write::{ZlibDecoder, ZlibEncoder},
    Compression,
};
use message::MessageValue;
use tracing::warn;

use super::Compresser;

struct HaliaZlibEncoder {
    field: String,
    target_field: String,
}

pub fn new_encoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaZlibEncoder {
        field,
        target_field,
    })
}

impl HaliaZlibEncoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut zliber = ZlibEncoder::new(vec![], Compression::default());
        zliber.write_all(bytes)?;
        let data = zliber.finish()?;
        Ok(data)
    }
}

impl Compresser for HaliaZlibEncoder {
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

struct HaliaZlibDecoder {
    field: String,
    target_field: String,
}

pub fn new_decoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaZlibDecoder {
        field,
        target_field,
    })
}

impl HaliaZlibDecoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut zliber = ZlibDecoder::new(vec![]);
        zliber.write_all(bytes)?;
        let data = zliber.finish()?;
        Ok(data)
    }
}

impl Compresser for HaliaZlibDecoder {
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

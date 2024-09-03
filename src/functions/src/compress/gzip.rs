use std::io::Write as _;

use anyhow::Result;
use flate2::{
    write::{GzDecoder, GzEncoder},
    Compression,
};
use message::MessageValue;
use tracing::warn;

use super::Compresser;

struct HaliaGzEncoder {
    field: String,
    target_field: String,
}

pub fn new_encoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaGzEncoder {
        field,
        target_field,
    })
}

impl HaliaGzEncoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut gzer = GzEncoder::new(vec![], Compression::default());
        gzer.write_all(bytes)?;
        let data = gzer.finish()?;
        Ok(data)
    }
}

impl Compresser for HaliaGzEncoder {
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

struct HaliaGzDecoder {
    field: String,
    target_field: String,
}

pub fn new_decoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaGzDecoder {
        field,
        target_field,
    })
}

impl HaliaGzDecoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut gzer = GzDecoder::new(vec![]);
        gzer.write_all(bytes)?;
        let data = gzer.finish()?;
        Ok(data)
    }
}

impl Compresser for HaliaGzDecoder {
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

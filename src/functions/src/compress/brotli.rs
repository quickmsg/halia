use std::io::{Read, Write as _};

use anyhow::Result;
use brotli::{CompressorWriter, Decompressor};
use message::MessageValue;
use tracing::warn;

use super::Compresser;

struct HaliaBrotliEncoder {
    field: String,
    target_field: String,
}

pub fn new_encoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaBrotliEncoder {
        field,
        target_field,
    })
}

impl HaliaBrotliEncoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut data = vec![];
        {
            let mut brotlier = CompressorWriter::new(&mut data, 4096, 11, 22);
            brotlier.write_all(bytes)?;
            brotlier.flush()?;
        }
        Ok(data)
    }
}

impl Compresser for HaliaBrotliEncoder {
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

struct HaliaBrotliDecoder {
    field: String,
    target_field: String,
}

pub fn new_decoder(field: String, target_field: String) -> Box<dyn Compresser> {
    Box::new(HaliaBrotliDecoder {
        field,
        target_field,
    })
}

impl HaliaBrotliDecoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut data = vec![];
        {
            let mut brotlier = Decompressor::new(bytes, 4096);
            brotlier.read_to_end(&mut data)?;
        }

        Ok(data)
    }
}

impl Compresser for HaliaBrotliDecoder {
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

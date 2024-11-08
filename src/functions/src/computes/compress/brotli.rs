use std::io::{Read, Write as _};

use anyhow::Result;
use brotli::{CompressorWriter, Decompressor};
use message::MessageValue;
use tracing::warn;
use types::rules::functions::computer::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaBrotliEncoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_encoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaBrotliEncoder {
        field: conf.field,
        target_field: conf.target_field,
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

impl Computer for HaliaBrotliEncoder {
    fn compute(&self, message: &mut message::Message) {
        let result = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(str) => match Self::encode(str.as_bytes()) {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                },
                message::MessageValue::Bytes(bytes) => match Self::encode(bytes) {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                },
                _ => return,
            },
            None => return,
        };

        add_or_set_message_value!(self, message, MessageValue::Bytes(result));
    }
}

struct HaliaBrotliDecoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_decoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaBrotliDecoder {
        field: conf.field,
        target_field: conf.target_field,
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

impl Computer for HaliaBrotliDecoder {
    fn compute(&self, message: &mut message::Message) {
        let result = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(str) => match Self::decode(str.as_bytes()) {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                },
                message::MessageValue::Bytes(bytes) => match Self::decode(bytes) {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                },
                _ => return,
            },
            None => return,
        };

        add_or_set_message_value!(self, message, MessageValue::Bytes(result));
    }
}

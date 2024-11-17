use anyhow::Result;
use message::MessageValue;
use snap::raw::{Decoder, Encoder};
use tracing::warn;
use types::rules::functions::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaSnappyEncoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_encoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaSnappyEncoder {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl HaliaSnappyEncoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut snappyer = Encoder::new();
        let data = snappyer.compress_vec(bytes)?;
        Ok(data)
    }
}

impl Computer for HaliaSnappyEncoder {
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

struct HaliaSnappyDecoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_decoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaSnappyDecoder {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl HaliaSnappyDecoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut snappyer = Decoder::new();
        let data = snappyer.decompress_vec(bytes)?;
        Ok(data)
    }
}

impl Computer for HaliaSnappyDecoder {
    fn compute(&self, message: &mut message::Message) {
        let result = match message.get(&self.field) {
            Some(mv) => match mv {
                message::MessageValue::String(str) => match Self::decode(str.as_bytes()) {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("decode err {}", e);
                        return;
                    }
                },
                message::MessageValue::Bytes(bytes) => match Self::decode(bytes) {
                    Ok(data) => data,
                    Err(e) => {
                        warn!("decode err {}", e);
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

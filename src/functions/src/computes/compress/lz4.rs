use anyhow::Result;
use lz4_flex::{compress, decompress};
use message::MessageValue;
use tracing::warn;
use types::rules::functions::computer::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaLz4Encoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_encoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaLz4Encoder {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl HaliaLz4Encoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        Ok(compress(bytes))
    }
}

impl Computer for HaliaLz4Encoder {
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

struct HaliaLz4Decoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_decoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaLz4Decoder {
        field: conf.field,
        target_field: conf.target_field,
    })
}

impl HaliaLz4Decoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let data = decompress(bytes, bytes.len() * 10)?;
        Ok(data)
    }
}

impl Computer for HaliaLz4Decoder {
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

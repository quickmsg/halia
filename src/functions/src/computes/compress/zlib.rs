use std::io::Write as _;

use anyhow::Result;
use flate2::{
    write::{ZlibDecoder, ZlibEncoder},
    Compression,
};
use message::MessageValue;
use tracing::warn;
use types::rules::functions::computer::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaZlibEncoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_encoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaZlibEncoder {
        field: conf.field,
        target_field: conf.target_field,
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

impl Computer for HaliaZlibEncoder {
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

struct HaliaZlibDecoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_decoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaZlibDecoder {
        field: conf.field,
        target_field: conf.target_field,
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

impl Computer for HaliaZlibDecoder {
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

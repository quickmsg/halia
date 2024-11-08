use std::io::Write as _;

use anyhow::Result;
use flate2::{
    write::{GzDecoder, GzEncoder},
    Compression,
};
use message::MessageValue;
use tracing::warn;
use types::rules::functions::computer::ItemConf;

use crate::{add_or_set_message_value, computes::Computer};

struct HaliaGzEncoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_encoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaGzEncoder {
        field: conf.field,
        target_field: conf.target_field,
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

impl Computer for HaliaGzEncoder {
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

struct HaliaGzDecoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_decoder(conf: ItemConf) -> Box<dyn Computer> {
    Box::new(HaliaGzDecoder {
        field: conf.field,
        target_field: conf.target_field,
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

impl Computer for HaliaGzDecoder {
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

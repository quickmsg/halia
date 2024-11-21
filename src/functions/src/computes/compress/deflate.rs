use std::io::Write as _;

use anyhow::Result;
use flate2::{
    write::{DeflateDecoder, DeflateEncoder},
    Compression,
};
use message::MessageValue;
use tracing::warn;

use crate::{
    add_or_set_message_value, computes::Computer, get_field_and_option_target_field, Args,
};

struct HaliaDeflateEncoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_encoder(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = get_field_and_option_target_field(&mut args)?;
    Ok(Box::new(HaliaDeflateEncoder {
        field,
        target_field,
    }))
}

impl HaliaDeflateEncoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut deflater = DeflateEncoder::new(vec![], Compression::default());
        deflater.write_all(bytes)?;
        let data = deflater.finish()?;
        Ok(data)
    }
}

impl Computer for HaliaDeflateEncoder {
    fn compute(&mut self, message: &mut message::Message) {
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

struct HaliaDeflateDecoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_decoder(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = get_field_and_option_target_field(&mut args)?;
    Ok(Box::new(HaliaDeflateDecoder {
        field,
        target_field,
    }))
}

impl HaliaDeflateDecoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut deflater = DeflateDecoder::new(vec![]);
        deflater.write_all(bytes)?;
        let data = deflater.finish()?;
        Ok(data)
    }
}

impl Computer for HaliaDeflateDecoder {
    fn compute(&mut self, message: &mut message::Message) {
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

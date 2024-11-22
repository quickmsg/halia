use anyhow::Result;
use message::MessageValue;
use snap::raw::{Decoder, Encoder};
use tracing::warn;

use crate::{add_or_set_message_value, computes::Computer, Args};

struct HaliaSnappyEncoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_encoder(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(HaliaSnappyEncoder {
        field,
        target_field,
    }))
}

impl HaliaSnappyEncoder {
    fn encode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut snappyer = Encoder::new();
        let data = snappyer.compress_vec(bytes)?;
        Ok(data)
    }
}

impl Computer for HaliaSnappyEncoder {
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

struct HaliaSnappyDecoder {
    field: String,
    target_field: Option<String>,
}

pub fn new_decoder(mut args: Args) -> Result<Box<dyn Computer>> {
    let (field, target_field) = args.take_field_and_option_target_field()?;
    Ok(Box::new(HaliaSnappyDecoder {
        field,
        target_field,
    }))
}

impl HaliaSnappyDecoder {
    fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
        let mut snappyer = Decoder::new();
        let data = snappyer.decompress_vec(bytes)?;
        Ok(data)
    }
}

impl Computer for HaliaSnappyDecoder {
    fn compute(&mut self, message: &mut message::Message) {
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

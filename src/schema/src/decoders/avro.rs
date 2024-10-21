use std::io::Cursor;

use anyhow::{bail, Result};
use apache_avro::{Reader, Schema};
use bytes::Bytes;
use common::error::HaliaResult;
use message::{Message, MessageBatch};
use tracing::warn;
use types::schema::AvroDecodeConf;

use crate::Decoder;

struct Avro;

pub(crate) fn validate_conf(conf: &serde_json::Value) -> Result<()> {
    let conf: AvroDecodeConf = serde_json::from_value(conf.clone())?;
    Schema::parse_str(&conf.schema)?;
    Ok(())
}

pub(crate) fn new() -> HaliaResult<Box<dyn Decoder>> {
    Ok(Box::new(Avro))
}

pub(crate) fn new_with_conf(conf: AvroDecodeConf) -> HaliaResult<Box<dyn Decoder>> {
    let schema = Schema::parse_str(&conf.schema).unwrap();
    Ok(Box::new(AvroWithSchema { schema }))
}

impl Decoder for Avro {
    fn decode(&self, data: Bytes) -> Result<MessageBatch> {
        let mut mb = MessageBatch::default();
        let reader = Reader::new(&data[..]).unwrap();
        for value in reader {
            match value {
                Ok(value) => {
                    let msg = Message::try_from(value)?;
                    mb.push_message(msg);
                }
                Err(e) => warn!("Error decoding avro: {:?}", e),
            }
        }

        Ok(mb)
    }
}

struct AvroWithSchema {
    schema: Schema,
}

impl Decoder for AvroWithSchema {
    fn decode(&self, data: Bytes) -> Result<MessageBatch> {
        let reader = Reader::with_schema(&self.schema, Cursor::new(data)).unwrap();
        let mut mb = MessageBatch::default();
        for value in reader {
            match value {
                Ok(v) => {
                    let msg = Message::try_from(v)?;
                    mb.push_message(msg);
                }
                Err(e) => bail!(e),
            }
        }
        Ok(mb)
    }
}

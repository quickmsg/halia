use std::io::Cursor;

use anyhow::{bail, Result};
use apache_avro::{Reader, Schema};
use bytes::Bytes;
use common::error::HaliaResult;
use message::{Message, MessageBatch};
use tracing::warn;
use types::schema::AvroDecodeConf;

use crate::Decoder;

pub struct Avro {
    schema: Option<Schema>,
}

pub(crate) fn validate_conf(conf: &serde_json::Value) -> Result<()> {
    let conf: AvroDecodeConf = serde_json::from_value(conf.clone())?;
    Schema::parse_str(&conf.schema)?;
    Ok(())
}

impl Avro {
    pub fn new() -> Box<dyn Decoder> {
        Box::new(Self { schema: None })
    }

    pub async fn new_with_conf(id: &String) -> HaliaResult<Box<dyn Decoder>> {
        let conf = storage::schema::read_conf(id).await?;
        let conf: AvroDecodeConf = serde_json::from_slice(&conf)?;
        let schema = Schema::parse_str(&conf.schema).unwrap();
        Ok(Box::new(Self {
            schema: Some(schema),
        }))
    }

    fn set_schema(&mut self, schema: &String) -> Result<()> {
        let schema = Schema::parse_str(schema)?;
        self.schema = Some(schema);
        Ok(())
    }
}

impl Decoder for Avro {
    fn decode(&self, data: Bytes) -> Result<MessageBatch> {
        match &self.schema {
            Some(schema) => {
                let reader = Reader::with_schema(schema, Cursor::new(data)).unwrap();
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
            None => {
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
    }
}

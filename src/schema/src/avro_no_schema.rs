use anyhow::Result;
use apache_avro::Reader;
use bytes::Bytes;
use message::{Message, MessageBatch};
use tracing::warn;

use crate::Coder;

pub struct AvroNoSchema;

impl Coder for AvroNoSchema {
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

    fn encode(&self, mb: MessageBatch) -> Result<Bytes> {
        todo!()
    }
}

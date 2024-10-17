use std::io::Cursor;

use anyhow::{bail, Result};
use message::{Message, MessageBatch};
use prost_reflect::{DescriptorPool, DynamicMessage};
use types::schema::ProtobufDecodeConf;

use crate::Decoder;

pub struct Protobuf {
    message_descriptor: prost_reflect::MessageDescriptor,
}

pub fn validate_conf(conf: &serde_json::Value) -> Result<()> {
    let conf: ProtobufDecodeConf = serde_json::from_value(conf.clone())?;
    let pool = DescriptorPool::decode(Cursor::new(conf.descriptor))?;
    if pool.get_message_by_name(&conf.message_type).is_none() {
        bail!("message type not found");
    };
    Ok(())
}

impl Protobuf {
    pub fn new(descriptor: String, message_type: String) -> Result<Self> {
        let pool = DescriptorPool::decode(Cursor::new(descriptor))?;
        let message_descriptor = match pool.get_message_by_name(&message_type) {
            Some(message_descriptor) => message_descriptor,
            None => bail!("message type not found"),
        };
        Ok(Self { message_descriptor })
    }
}

impl Decoder for Protobuf {
    fn decode(&self, data: bytes::Bytes) -> Result<message::MessageBatch> {
        let mut dynamic_message =
            DynamicMessage::decode(self.message_descriptor.clone(), data).unwrap();
        let mut mb = MessageBatch::default();
        for (field, value) in dynamic_message.take_fields() {
            mb.push_message(Message::from(value));
        }

        Ok(mb)
    }
}

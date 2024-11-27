use message::{Message, MessageBatch};

use crate::{Decoder, DEFAULT_KEY};

pub struct Toml;

impl Decoder for Toml {
    fn decode(&self, data: bytes::Bytes) -> anyhow::Result<message::MessageBatch> {
        let data: toml::Value = toml::from_str(std::str::from_utf8(&data)?)?;
        match data {
            toml::Value::Array(vec) => {
                let mut mb = MessageBatch::default();
                for value in vec {
                    mb.push_message(Message::from(value));
                }
                Ok(mb)
            }
            toml::Value::Table(map) => {
                let mut mb = MessageBatch::default();
                let mut message = Message::default();
                for (key, value) in map {
                    message.add(key, value.into());
                }
                mb.push_message(message);
                Ok(mb)
            }
            _ => {
                let mut mb = MessageBatch::default();
                let mut message = Message::default();
                message.add(DEFAULT_KEY.to_string(), data.into());
                mb.push_message(message);
                Ok(mb)
            }
        }
    }
}

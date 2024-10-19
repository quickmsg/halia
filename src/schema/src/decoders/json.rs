use anyhow::bail;
use message::{Message, MessageBatch};

use crate::Decoder;

pub struct Json;

impl Json {
    pub fn new() -> Box<dyn Decoder> {
        Box::new(Self)
    }
}

impl Decoder for Json {
    fn decode(&self, data: bytes::Bytes) -> anyhow::Result<message::MessageBatch> {
        let data: serde_json::Value = serde_json::from_slice(&data)?;
        match data {
            serde_json::Value::Array(datas) => {
                let mut mb = MessageBatch::default();
                for data in datas {
                    mb.push_message(Message::from(data));
                }
                Ok(mb)
            }
            serde_json::Value::Object(_) => {
                let mut mb = MessageBatch::default();
                mb.push_message(Message::from(data));
                Ok(mb)
            }
            _ => bail!("not valid json data"),
        }
    }
}

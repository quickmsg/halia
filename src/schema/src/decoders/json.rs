use anyhow::bail;
use common::error::HaliaResult;
use message::{Message, MessageBatch};

use crate::Decoder;

struct Json;

pub fn new() -> HaliaResult<Box<dyn Decoder>> {
    Ok(Box::new(Json))
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

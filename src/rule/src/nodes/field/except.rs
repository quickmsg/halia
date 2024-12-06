use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;
use types::rule::Operate;

pub struct Except {
    fields: Vec<String>,
}

impl Except {
    pub fn new(conf: Value) -> Result<Except> {
        let fields: Vec<String> = serde_json::from_value(conf)?;
        Ok(Except { fields })
    }
}

impl Operate for Except {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        for message in messages {
            message.except(&self.fields);
        }
        true
    }
}
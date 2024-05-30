use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;
use types::graph::Operate;

pub struct Remove {
    fields: Vec<String>,
}

impl Remove {
    pub fn new(conf: Value) -> Result<Remove> {
        let fields: Vec<String> = serde_json::from_value(conf)?;
        Ok(Remove { fields })
    }
}

impl Operate for Remove {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        for message in messages {
            message.remove(&self.fields);
        }

        true
    }
}

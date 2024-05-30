use std::collections::HashMap;

use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;
use types::graph::Operate;

pub struct Rename {
    fields: HashMap<String, String>,
}

impl Rename {
    pub fn new(conf: Value) -> Result<Rename> {
        let fields: HashMap<String, String> = serde_json::from_value(conf)?;
        Ok(Rename { fields })
    }
}

impl Operate for Rename {
    fn operate(&self, message_batch: &mut MessageBatch) -> bool {
        let messages = message_batch.get_messages_mut();
        for message in messages {
            message.rename(&self.fields);
        }
        true
    }
}

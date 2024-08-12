use std::collections::HashMap;

use anyhow::Result;
use message::MessageBatch;
use serde_json::Value;

use super::Operator;

pub struct Move {
    fields: HashMap<String, String>,
}

pub fn new(conf: Value) -> Result<Box<dyn Operator>> {
    let fields: HashMap<String, String> = serde_json::from_value(conf)?;
    // Ok(Move { fields })
    todo!()
}

impl Operator for Move {
    fn operate(&self, msg: &mut message::Message) {
        todo!()
    }
}

// impl Operate for Rename {
//     fn operate(&self, message_batch: &mut MessageBatch) -> bool {
//         let messages = message_batch.get_messages_mut();
//         for message in messages {
//             message.rename(&self.fields);
//         }
//         true
//     }
// }

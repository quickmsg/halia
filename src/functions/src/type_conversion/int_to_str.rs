use anyhow::Result;
use message::{MessageBatch, MessageValue};
use serde_json::Value;

use crate::{add_or_set_message_value, Function};

pub struct IntToString {
    field: String,
    target_field: Option<String>,
}

impl IntToString {
    pub fn new(field: String, conf: Value) -> Result<Self> {
        todo!()
        // let values: Vec<Value> = serde_json::from_value(conf)?;
        // Ok(Append { field, values })
    }
}

impl Function for IntToString {
    fn call(&self, message_batch: &mut MessageBatch) -> bool {
        for message in message_batch.get_messages_mut() {
            match message.get(&self.field) {
                Some(value) => match value {
                    message::MessageValue::Int64(i) => {
                        add_or_set_message_value!(
                            self,
                            message,
                            MessageValue::String(format!("{}", i))
                        )
                    }
                    _ => add_or_set_message_value!(self, message, MessageValue::Null),
                },
                None => add_or_set_message_value!(self, message, MessageValue::Null),
            }
        }
        true
    }
}
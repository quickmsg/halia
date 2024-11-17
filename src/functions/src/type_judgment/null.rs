use anyhow::Result;
use async_trait::async_trait;
use message::{MessageBatch, MessageValue};
use serde_json::Value;

use crate::{add_or_set_message_value, Function};

pub struct IsNull {
    field: String,
    target_field: Option<String>,
}

impl IsNull {
    pub fn new(field: String, conf: Value) -> Result<Self> {
        todo!()
        // let values: Vec<Value> = serde_json::from_value(conf)?;
        // Ok(Append { field, values })
    }
}

#[async_trait]
impl Function for IsNull {
    async fn call(&mut self, message_batch: &mut MessageBatch) -> bool {
        for message in message_batch.get_messages_mut() {
            match message.get(&self.field) {
                Some(value) => match value {
                    message::MessageValue::Null => {
                        add_or_set_message_value!(self, message, MessageValue::Boolean(true))
                    }
                    _ => add_or_set_message_value!(self, message, MessageValue::Boolean(false)),
                },
                None => add_or_set_message_value!(self, message, MessageValue::Boolean(false)),
            }
        }
        true
    }
}

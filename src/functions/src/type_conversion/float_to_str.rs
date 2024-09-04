use anyhow::Result;
use message::{MessageBatch, MessageValue};
use serde_json::Value;

use crate::{add_or_set_message_value, Function};

pub struct FloatToString {
    field: String,
    target_field: Option<String>,
    precision: usize,
}

impl FloatToString {
    pub fn new(field: String, conf: Value) -> Result<Self> {
        todo!()
        // let values: Vec<Value> = serde_json::from_value(conf)?;
        // Ok(Append { field, values })
    }
}

impl Function for FloatToString {
    fn call(&self, message_batch: &mut MessageBatch) -> bool {
        for message in message_batch.get_messages_mut() {
            match message.get(&self.field) {
                Some(value) => match value {
                    message::MessageValue::Float64(f) => {
                        add_or_set_message_value!(
                            self,
                            message,
                            MessageValue::String(format!(
                                "{:.precision$}",
                                f,
                                precision = self.precision
                            ))
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
